import { proto } from '../../WAProto/index.js'
import type { GroupMetadata, GroupParticipant, ParticipantAction, SocketConfig, WAMessageKey } from '../Types'
import { WAMessageAddressingMode, WAMessageStubType } from '../Types'
import { generateMessageIDV2, unixTimestampSeconds } from '../Utils'
import { Mutex } from 'async-mutex'
import { mkdir, readFile, writeFile } from 'fs/promises'
import { LRUCache } from 'lru-cache'
import { dirname } from 'path'
import {
	type BinaryNode,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	getBinaryNodeChildString,
	isLidUser,
	isPnUser,
	jidEncode,
	jidNormalizedUser
} from '../WABinary'
import { makeChatsSocket } from './chats'

// We need to lock cache files due to async read/write (see use-multi-file-auth-state)
const fileLocks = new Map<string, Mutex>()
const getFileLock = (path: string): Mutex => {
	let mutex = fileLocks.get(path)
	if (!mutex) {
		mutex = new Mutex()
		fileLocks.set(path, mutex)
	}

	return mutex
}

export const makeGroupsSocket = (config: SocketConfig) => {
	const sock = makeChatsSocket(config)
	const { authState, ev, query, upsertMessage } = sock
	const {
		logger,
		cachedGroupMetadata,
		groupMetadataCacheTtlMs,
		groupMetadataCacheMaxSize,
		groupMetadataCacheFile,
		groupMetadataCacheSaveDebounceMs
	} = config

	const normalizeGroupJid = (jid: string) => (jid.includes('@') ? jid : jidEncode(jid, 'g.us'))

	// ---- GROUP METADATA CACHE (dramatically reduces requests to WA) ----
	const groupMetaCache =
		groupMetadataCacheTtlMs > 0
			? new LRUCache<string, GroupMetadata>({
				max: Math.max(16, groupMetadataCacheMaxSize || 0),
				ttl: groupMetadataCacheTtlMs,
				updateAgeOnGet: true
			})
			: undefined
	const inFlightGroupMeta = new Map<string, Promise<GroupMetadata>>()

	// Optional persistence of the in-memory group metadata cache.
	// Useful for bots that restart often: avoids a burst of WA metadata queries after reboot.
	const persistPath = groupMetadataCacheFile
	const persistDebounceMs = Math.max(250, groupMetadataCacheSaveDebounceMs ?? 5000)
	const persistMutex = persistPath ? getFileLock(persistPath) : undefined
	let persistDirty = false
	let persistTimer: NodeJS.Timeout | undefined

	const persistGroupMetaCache = async () => {
		if (!groupMetaCache || !persistPath || !persistMutex || !persistDirty) return

		persistDirty = false
		const release = await persistMutex.acquire()
		try {
			await mkdir(dirname(persistPath), { recursive: true }).catch(() => {})
			const dump = groupMetaCache.dump()
			await writeFile(persistPath, JSON.stringify({ v: 1, dump }), { encoding: 'utf-8' })
		} catch (err) {
			logger?.trace({ err, file: persistPath }, 'failed to persist group metadata cache')
		} finally {
			release()
		}
	}

	const markGroupMetaCacheDirty = () => {
		if (!groupMetaCache || !persistPath) return
		persistDirty = true
		if (persistTimer) return

		persistTimer = setTimeout(() => {
			persistTimer = undefined
			void persistGroupMetaCache()
		}, persistDebounceMs)
		persistTimer.unref?.()
	}

	const groupMetaCacheHydration: Promise<void> = (async () => {
		if (!groupMetaCache || !persistPath || !persistMutex) return

		const release = await persistMutex.acquire()
		try {
			const raw = await readFile(persistPath, { encoding: 'utf-8' })
			const json = JSON.parse(raw) as unknown
			const dump = (json as any)?.dump
			// Accept both { dump: ... } and a raw dump array for backwards compatibility.
			const data = Array.isArray(dump) ? dump : Array.isArray(json) ? (json as any) : undefined
			if (data) {
				groupMetaCache.load(data)
				logger?.debug({ file: persistPath, count: groupMetaCache.size }, 'loaded group metadata cache')
			}
		} catch (err: any) {
			if (err?.code !== 'ENOENT') {
				logger?.trace({ err, file: persistPath }, 'failed to load group metadata cache')
			}
		} finally {
			release()
		}
	})()

	const setCachedGroupMetadata = (id: string, meta: GroupMetadata) => {
		if (!groupMetaCache) return
		groupMetaCache.set(id, meta)
		markGroupMetaCacheDirty()
	}

	const mergeGroupUpdate = (id: string, update: Partial<GroupMetadata>) => {
		if (!groupMetaCache) return
		const existing = groupMetaCache.get(id)
		if (!existing) return
		setCachedGroupMetadata(id, { ...existing, ...update, id })
	}

	const applyParticipantsUpdate = (
		id: string,
		action: ParticipantAction,
		participants: GroupParticipant[]
	) => {
		if (!groupMetaCache) return
		const existing = groupMetaCache.get(id)
		if (!existing?.participants) return

		const list = [...existing.participants]
		const findIndex = (p: GroupParticipant) =>
			list.findIndex(
				x =>
					x.id === p.id ||
					(!!p.phoneNumber && (x.id === p.phoneNumber || x.phoneNumber === p.phoneNumber)) ||
					(!!p.lid && (x.id === p.lid || x.lid === p.lid))
			)

		switch (action) {
			case 'add': {
				for (const p of participants) {
					const idx = findIndex(p)
					if (idx >= 0) list[idx] = { ...list[idx], ...p }
					else list.push(p)
				}
				break
			}
			case 'remove': {
				for (const p of participants) {
					const idx = findIndex(p)
					if (idx >= 0) list.splice(idx, 1)
				}
				break
			}
			case 'promote':
			case 'demote':
			case 'modify': {
				for (const p of participants) {
					const idx = findIndex(p)
					if (idx >= 0) list[idx] = { ...list[idx], ...p }
					else list.push(p)
				}
				break
			}
		}

		setCachedGroupMetadata(id, { ...existing, participants: list, size: list.length, id })
	}

	// Keep cache fresh via events (no extra WA queries)
	ev.on('groups.update', (updates: any) => {
		if (!groupMetaCache || !Array.isArray(updates)) return
		for (const u of updates) {
			if (!u?.id) continue
			const { id, author, authorPn, ...rest } = u
			mergeGroupUpdate(id, rest)
		}
	})

	ev.on('group-participants.update', (update: any) => {
		if (!groupMetaCache || !update?.id || !Array.isArray(update?.participants) || !update?.action) return
		applyParticipantsUpdate(update.id, update.action, update.participants)
	})

	const groupQuery = async (jid: string, type: 'get' | 'set', content: BinaryNode[]) =>
		query({
			tag: 'iq',
			attrs: {
				type,
				xmlns: 'w:g2',
				to: jid
			},
			content
		})

	const fetchGroupMetadata = async (jid: string) => {
		const result = await groupQuery(jid, 'get', [{ tag: 'query', attrs: { request: 'interactive' } }])
		return extractGroupMetadata(result)
	}

	const groupMetadata = async (jid: string) => {
		await groupMetaCacheHydration
		jid = normalizeGroupJid(jid)
		const cached = groupMetaCache?.get(jid)
		if (cached) return cached

		const inflight = inFlightGroupMeta.get(jid)
		if (inflight) return inflight

		const p = (async () => {
			// allow user-provided cache hook first
			try {
				const external = await cachedGroupMetadata(jid)
				if (external) {
					setCachedGroupMetadata(jid, external)
					return external
				}
			} catch (err) {
				logger?.trace({ err, jid }, 'cachedGroupMetadata hook failed')
			}

			const meta = await fetchGroupMetadata(jid)
			setCachedGroupMetadata(jid, meta)
			return meta
		})()

		inFlightGroupMeta.set(jid, p)
		try {
			return await p
		} finally {
			inFlightGroupMeta.delete(jid)
		}
	}

	const groupFetchAllParticipating = async () => {
		const result = await query({
			tag: 'iq',
			attrs: {
				to: '@g.us',
				xmlns: 'w:g2',
				type: 'get'
			},
			content: [
				{
					tag: 'participating',
					attrs: {},
					content: [
						{ tag: 'participants', attrs: {} },
						{ tag: 'description', attrs: {} }
					]
				}
			]
		})
		const data: { [_: string]: GroupMetadata } = {}
		const groupsChild = getBinaryNodeChild(result, 'groups')
		if (groupsChild) {
			const groups = getBinaryNodeChildren(groupsChild, 'group')
			for (const groupNode of groups) {
				const meta = extractGroupMetadata({
					tag: 'result',
					attrs: {},
					content: [groupNode]
				})
				data[meta.id] = meta
				setCachedGroupMetadata(meta.id, meta)
			}
		}

		// TODO: properly parse LID / PN DATA
		sock.ev.emit('groups.update', Object.values(data))

		return data
	}

	sock.ws.on('CB:ib,,dirty', async (node: BinaryNode) => {
		const { attrs } = getBinaryNodeChild(node, 'dirty')!
		if (attrs.type !== 'groups') {
			return
		}

		await groupFetchAllParticipating()
		await sock.cleanDirtyBits('groups')
	})

	return {
		...sock,
		groupMetadata,
		groupCreate: async (subject: string, participants: string[]) => {
			const key = generateMessageIDV2()
			const result = await groupQuery('@g.us', 'set', [
				{
					tag: 'create',
					attrs: {
						subject,
						key
					},
					content: participants.map(jid => ({
						tag: 'participant',
						attrs: { jid }
					}))
				}
			])
			const meta = extractGroupMetadata(result)
			setCachedGroupMetadata(meta.id, meta)
			return meta
		},
		groupLeave: async (id: string) => {
			await groupQuery('@g.us', 'set', [
				{
					tag: 'leave',
					attrs: {},
					content: [{ tag: 'group', attrs: { id } }]
				}
			])
		},
		groupUpdateSubject: async (jid: string, subject: string) => {
			await groupQuery(jid, 'set', [
				{
					tag: 'subject',
					attrs: {},
					content: Buffer.from(subject, 'utf-8')
				}
			])
		},
		groupRequestParticipantsList: async (jid: string) => {
			const result = await groupQuery(jid, 'get', [
				{
					tag: 'membership_approval_requests',
					attrs: {}
				}
			])
			const node = getBinaryNodeChild(result, 'membership_approval_requests')
			const participants = getBinaryNodeChildren(node, 'membership_approval_request')
			return participants.map(v => v.attrs)
		},
		groupRequestParticipantsUpdate: async (jid: string, participants: string[], action: 'approve' | 'reject') => {
			const result = await groupQuery(jid, 'set', [
				{
					tag: 'membership_requests_action',
					attrs: {},
					content: [
						{
							tag: action,
							attrs: {},
							content: participants.map(jid => ({
								tag: 'participant',
								attrs: { jid }
							}))
						}
					]
				}
			])
			const node = getBinaryNodeChild(result, 'membership_requests_action')
			const nodeAction = getBinaryNodeChild(node, action)
			const participantsAffected = getBinaryNodeChildren(nodeAction, 'participant')
			return participantsAffected.map(p => {
				return { status: p.attrs.error || '200', jid: p.attrs.jid }
			})
		},
		groupParticipantsUpdate: async (jid: string, participants: string[], action: ParticipantAction) => {
			const result = await groupQuery(jid, 'set', [
				{
					tag: action,
					attrs: {},
					content: participants.map(jid => ({
						tag: 'participant',
						attrs: { jid }
					}))
				}
			])
			const node = getBinaryNodeChild(result, action)
			const participantsAffected = getBinaryNodeChildren(node, 'participant')
			return participantsAffected.map(p => {
				return { status: p.attrs.error || '200', jid: p.attrs.jid, content: p }
			})
		},
		groupUpdateDescription: async (jid: string, description?: string) => {
			// IMPORTANT: fetch fresh metadata here to avoid conflicts with stale descId
			const metadata = await fetchGroupMetadata(normalizeGroupJid(jid))
			const prev = metadata.descId ?? null

			await groupQuery(jid, 'set', [
				{
					tag: 'description',
					attrs: {
						...(description ? { id: generateMessageIDV2() } : { delete: 'true' }),
						...(prev ? { prev } : {})
					},
					content: description ? [{ tag: 'body', attrs: {}, content: Buffer.from(description, 'utf-8') }] : undefined
				}
			])
		},
		groupInviteCode: async (jid: string) => {
			const result = await groupQuery(jid, 'get', [{ tag: 'invite', attrs: {} }])
			const inviteNode = getBinaryNodeChild(result, 'invite')
			return inviteNode?.attrs.code
		},
		groupRevokeInvite: async (jid: string) => {
			const result = await groupQuery(jid, 'set', [{ tag: 'invite', attrs: {} }])
			const inviteNode = getBinaryNodeChild(result, 'invite')
			return inviteNode?.attrs.code
		},
		groupAcceptInvite: async (code: string) => {
			const results = await groupQuery('@g.us', 'set', [{ tag: 'invite', attrs: { code } }])
			const result = getBinaryNodeChild(results, 'group')
			return result?.attrs.jid
		},

		/**
		 * revoke a v4 invite for someone
		 * @param groupJid group jid
		 * @param invitedJid jid of person you invited
		 * @returns true if successful
		 */
		groupRevokeInviteV4: async (groupJid: string, invitedJid: string) => {
			const result = await groupQuery(groupJid, 'set', [
				{ tag: 'revoke', attrs: {}, content: [{ tag: 'participant', attrs: { jid: invitedJid } }] }
			])
			return !!result
		},

		/**
		 * accept a GroupInviteMessage
		 * @param key the key of the invite message, or optionally only provide the jid of the person who sent the invite
		 * @param inviteMessage the message to accept
		 */
		groupAcceptInviteV4: ev.createBufferedFunction(
			async (key: string | WAMessageKey, inviteMessage: proto.Message.IGroupInviteMessage) => {
				key = typeof key === 'string' ? { remoteJid: key } : key
				const results = await groupQuery(inviteMessage.groupJid!, 'set', [
					{
						tag: 'accept',
						attrs: {
							code: inviteMessage.inviteCode!,
							expiration: inviteMessage.inviteExpiration!.toString(),
							admin: key.remoteJid!
						}
					}
				])

				// if we have the full message key
				// update the invite message to be expired
				if (key.id) {
					// create new invite message that is expired
					inviteMessage = proto.Message.GroupInviteMessage.fromObject(inviteMessage)
					inviteMessage.inviteExpiration = 0
					inviteMessage.inviteCode = ''
					ev.emit('messages.update', [
						{
							key,
							update: {
								message: {
									groupInviteMessage: inviteMessage
								}
							}
						}
					])
				}

				// generate the group add message
				await upsertMessage(
					{
						key: {
							remoteJid: inviteMessage.groupJid,
							id: generateMessageIDV2(sock.user?.id),
							fromMe: false,
							participant: key.remoteJid
						},
						messageStubType: WAMessageStubType.GROUP_PARTICIPANT_ADD,
						messageStubParameters: [JSON.stringify(authState.creds.me)],
						participant: key.remoteJid,
						messageTimestamp: unixTimestampSeconds()
					},
					'notify'
				)

				return results.attrs.from
			}
		),
		groupGetInviteInfo: async (code: string) => {
			const results = await groupQuery('@g.us', 'get', [{ tag: 'invite', attrs: { code } }])
			return extractGroupMetadata(results)
		},
		groupToggleEphemeral: async (jid: string, ephemeralExpiration: number) => {
			const content: BinaryNode = ephemeralExpiration
				? { tag: 'ephemeral', attrs: { expiration: ephemeralExpiration.toString() } }
				: { tag: 'not_ephemeral', attrs: {} }
			await groupQuery(jid, 'set', [content])
		},
		groupSettingUpdate: async (jid: string, setting: 'announcement' | 'not_announcement' | 'locked' | 'unlocked') => {
			await groupQuery(jid, 'set', [{ tag: setting, attrs: {} }])
		},
		groupMemberAddMode: async (jid: string, mode: 'admin_add' | 'all_member_add') => {
			await groupQuery(jid, 'set', [{ tag: 'member_add_mode', attrs: {}, content: mode }])
		},
		groupJoinApprovalMode: async (jid: string, mode: 'on' | 'off') => {
			await groupQuery(jid, 'set', [
				{ tag: 'membership_approval_mode', attrs: {}, content: [{ tag: 'group_join', attrs: { state: mode } }] }
			])
		},
		groupFetchAllParticipating
	}
}

export const extractGroupMetadata = (result: BinaryNode) => {
	const group = getBinaryNodeChild(result, 'group')!
	const descChild = getBinaryNodeChild(group, 'description')
	let desc: string | undefined
	let descId: string | undefined
	let descOwner: string | undefined
	let descOwnerPn: string | undefined
	let descTime: number | undefined
	if (descChild) {
		desc = getBinaryNodeChildString(descChild, 'body')
		descOwner = descChild.attrs.participant ? jidNormalizedUser(descChild.attrs.participant) : undefined
		descOwnerPn = descChild.attrs.participant_pn ? jidNormalizedUser(descChild.attrs.participant_pn) : undefined
		descTime = +descChild.attrs.t!
		descId = descChild.attrs.id
	}

	const groupId = group.attrs.id!.includes('@') ? group.attrs.id : jidEncode(group.attrs.id!, 'g.us')
	const eph = getBinaryNodeChild(group, 'ephemeral')?.attrs.expiration
	const memberAddMode = getBinaryNodeChildString(group, 'member_add_mode') === 'all_member_add'
	const metadata: GroupMetadata = {
		id: groupId!,
		notify: group.attrs.notify,
		addressingMode: group.attrs.addressing_mode === 'lid' ? WAMessageAddressingMode.LID : WAMessageAddressingMode.PN,
		subject: group.attrs.subject!,
		subjectOwner: group.attrs.s_o,
		subjectOwnerPn: group.attrs.s_o_pn,
		subjectTime: +group.attrs.s_t!,
		size: group.attrs.size ? +group.attrs.size : getBinaryNodeChildren(group, 'participant').length,
		creation: +group.attrs.creation!,
		owner: group.attrs.creator ? jidNormalizedUser(group.attrs.creator) : undefined,
		ownerPn: group.attrs.creator_pn ? jidNormalizedUser(group.attrs.creator_pn) : undefined,
		owner_country_code: group.attrs.creator_country_code,
		desc,
		descId,
		descOwner,
		descOwnerPn,
		descTime,
		linkedParent: getBinaryNodeChild(group, 'linked_parent')?.attrs.jid || undefined,
		restrict: !!getBinaryNodeChild(group, 'locked'),
		announce: !!getBinaryNodeChild(group, 'announcement'),
		isCommunity: !!getBinaryNodeChild(group, 'parent'),
		isCommunityAnnounce: !!getBinaryNodeChild(group, 'default_sub_group'),
		joinApprovalMode: !!getBinaryNodeChild(group, 'membership_approval_mode'),
		memberAddMode,
		participants: getBinaryNodeChildren(group, 'participant').map(({ attrs }) => {
			// TODO: Store LID MAPPINGS
			return {
				id: attrs.jid!,
				phoneNumber: isLidUser(attrs.jid) && isPnUser(attrs.phone_number) ? attrs.phone_number : undefined,
				lid: isPnUser(attrs.jid) && isLidUser(attrs.lid) ? attrs.lid : undefined,
				admin: (attrs.type || null) as GroupParticipant['admin']
			}
		}),
		ephemeralDuration: eph ? +eph : undefined
	}
	return metadata
}

export type GroupsSocket = ReturnType<typeof makeGroupsSocket>
