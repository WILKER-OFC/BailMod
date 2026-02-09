import type { NewsletterCreateResponse, NewsletterViewRole, SocketConfig, WAMediaUpload, WAMessage } from '../Types'
import type { NewsletterMetadata, NewsletterUpdate } from '../Types'
import { QueryIds, XWAPaths } from '../Types'
import { decryptMessageNode } from '../Utils'
import { generateProfilePicture } from '../Utils/messages-media'
import type { BinaryNode } from '../WABinary'
import {
	getAllBinaryNodeChildren,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	isJidNewsletter,
	S_WHATSAPP_NET
} from '../WABinary'
import { LRUCache } from 'lru-cache'
import { makeGroupsSocket } from './groups'
import { executeWMexQuery as genericExecuteWMexQuery } from './mex'

export type NewsletterFetchedMessage = {
	server_id: string
	views: number
	reactions: { code: string; count: number }[]
	message?: WAMessage
}

const parseNewsletterCreateResponse = (response: NewsletterCreateResponse): NewsletterMetadata => {
	const { id, thread_metadata: thread, viewer_metadata: viewer } = response
	return {
		id: id,
		owner: undefined,
		name: thread.name.text,
		creation_time: parseInt(thread.creation_time, 10),
		description: thread.description.text,
		invite: thread.invite,
		subscribers: parseInt(thread.subscribers_count, 10),
		verification: thread.verification,
		picture: {
			id: thread.picture.id,
			directPath: thread.picture.direct_path
		},
		mute_state: viewer.mute
	}
}

const parseNewsletterMetadata = (result: unknown): NewsletterMetadata | null => {
	if (typeof result !== 'object' || result === null) {
		return null
	}

	if ('id' in result && typeof result.id === 'string') {
		return result as NewsletterMetadata
	}

	if ('result' in result && typeof result.result === 'object' && result.result !== null && 'id' in result.result) {
		return result.result as NewsletterMetadata
	}

	return null
}

export const makeNewsletterSocket = (config: SocketConfig) => {
	const sock = makeGroupsSocket(config)
	const { authState, ev, query, generateMessageTag, signalRepository } = sock
	const { logger, newsletterMetadataCacheTtlMs, newsletterMetadataCacheMaxSize } = config

	const executeWMexQuery = <T>(variables: Record<string, unknown>, queryId: string, dataPath: string): Promise<T> => {
		return genericExecuteWMexQuery<T>(variables, queryId, dataPath, query, generateMessageTag)
	}

	const newsletterUpdate = async (jid: string, updates: NewsletterUpdate) => {
		const { settings, ...rest } = updates
		const variables = {
			newsletter_id: jid,
			updates: {
				...rest,
				// keep backwards-compat: `settings: null` unless explicitly provided
				settings: typeof settings === 'undefined' ? null : settings
			}
		}
		return executeWMexQuery(variables, QueryIds.UPDATE_METADATA, 'xwa2_newsletter_update')
	}

	const newsletterQuery = async (jid: string, type: 'get' | 'set', content: BinaryNode[]) =>
		query({
			tag: 'iq',
			attrs: {
				id: generateMessageTag(),
				type,
				xmlns: 'newsletter',
				to: jid
			},
			content
		})

	// ---- NEWSLETTER METADATA CACHE (reduces mex metadata spam) ----
	const newsletterMetaCache =
		newsletterMetadataCacheTtlMs > 0
			? new LRUCache<string, NewsletterMetadata>({
				max: Math.max(16, newsletterMetadataCacheMaxSize || 0),
				ttl: newsletterMetadataCacheTtlMs,
				updateAgeOnGet: true
			})
			: undefined
	const inviteToNewsletterJidCache =
		newsletterMetadataCacheTtlMs > 0
			? new LRUCache<string, string>({
				max: Math.max(16, Math.min(512, newsletterMetadataCacheMaxSize || 0)),
				ttl: newsletterMetadataCacheTtlMs,
				updateAgeOnGet: true
			})
			: undefined
	const inFlightNewsletterMeta = new Map<string, Promise<NewsletterMetadata | null>>()

	const mergeNewsletterSettingsUpdate = (jid: string, update: any) => {
		if (!newsletterMetaCache || !update || typeof update !== 'object') return
		const existing = newsletterMetaCache.get(jid)
		if (!existing) return

		const next: NewsletterMetadata = { ...existing }
		if (typeof update.name === 'string') next.name = update.name
		if (typeof update.description === 'string') next.description = update.description
		newsletterMetaCache.set(jid, next)
	}

	// Keep cache fresh via events (no extra WA queries)
	ev.on('newsletter-settings.update', ({ id, update }) => {
		mergeNewsletterSettingsUpdate(id, update)
	})

	const parseFetchedMessages = async (
		node: BinaryNode,
		mode: 'messages' | 'updates',
		{ decrypt }: { decrypt: boolean }
	): Promise<NewsletterFetchedMessage[]> => {
		const messagesNode =
			mode === 'messages'
				? getBinaryNodeChild(node, 'messages')
				: getBinaryNodeChild(getBinaryNodeChild(node, 'message_updates'), 'messages')

		if (!messagesNode) return []

		const fromJid = messagesNode.attrs.jid

		return await Promise.all(
			getAllBinaryNodeChildren(messagesNode).map(async messageNode => {
				// Some responses omit "from" on child message nodes; decoding relies on it.
				if (fromJid && !messageNode.attrs.from) {
					messageNode.attrs.from = fromJid
				}

				const views = parseInt(getBinaryNodeChild(messageNode, 'views_count')?.attrs?.count || '0', 10)
				const reactionNode = getBinaryNodeChild(messageNode, 'reactions')
				const reactions = getBinaryNodeChildren(reactionNode, 'reaction').map(({ attrs }) => ({
					count: parseInt(attrs.count || '0', 10),
					code: attrs.code || ''
				}))

				const server_id = messageNode.attrs.server_id || messageNode.attrs.message_id || messageNode.attrs.id || ''
				const data: NewsletterFetchedMessage = {
					server_id,
					views,
					reactions
				}

				if (decrypt) {
					const meId = authState.creds.me!.id
					const meLid = authState.creds.me!.lid || ''
					const { fullMessage, decrypt: doDecrypt } = decryptMessageNode(
						messageNode,
						meId,
						meLid,
						signalRepository,
						logger
					)
					await doDecrypt()
					data.message = fullMessage
				}

				return data
			})
		)
	}

	const getNewsletterMetadata = async (type: 'invite' | 'jid', key: string, viewRole?: NewsletterViewRole) => {
		// fast-path cache
		if (type === 'jid') {
			const cached = newsletterMetaCache?.get(key)
			if (cached) return cached
		} else {
			const mapped = inviteToNewsletterJidCache?.get(key)
			if (mapped) {
				const cached = newsletterMetaCache?.get(mapped)
				if (cached) return cached
			}
		}

		const inflightKey = `${type}:${key}:${viewRole || ''}`
		const inflight = inFlightNewsletterMeta.get(inflightKey)
		if (inflight) return inflight

		const p = (async () => {
			const variables: any = {
				fetch_creation_time: true,
				fetch_full_image: true,
				fetch_viewer_metadata: true,
				input: {
					key,
					type: type.toUpperCase()
				}
			}

			if (viewRole) {
				variables.input.view_role = viewRole
			}

			const result = await executeWMexQuery<unknown>(variables, QueryIds.METADATA, XWAPaths.xwa2_newsletter_metadata)
			const parsed = parseNewsletterMetadata(result)

			if (parsed?.id) {
				newsletterMetaCache?.set(parsed.id, parsed)
				if (type === 'invite') {
					inviteToNewsletterJidCache?.set(key, parsed.id)
				}
			}

			return parsed
		})()

		inFlightNewsletterMeta.set(inflightKey, p)
		try {
			return await p
		} finally {
			inFlightNewsletterMeta.delete(inflightKey)
		}
	}

	function newsletterFetchMessages(
		type: 'invite' | 'jid',
		key: string,
		count: number,
		after?: number
	): Promise<NewsletterFetchedMessage[]>
	function newsletterFetchMessages(
		jid: string,
		count: number,
		since?: number,
		after?: number
	): Promise<BinaryNode>
	async function newsletterFetchMessages(...args: any[]): Promise<NewsletterFetchedMessage[] | BinaryNode> {
		// WaBail-style: (type, key, count, after?) => parsed messages
		if (args[0] === 'invite' || args[0] === 'jid') {
			const [type, key, count, after] = args as ['invite' | 'jid', string, number, number?]
			const attrs: Record<string, string> = {
				type,
				count: count.toString()
			}

			if (type === 'invite') {
				attrs.key = key
			} else {
				attrs.jid = key
			}

			if (typeof after === 'number') {
				attrs.after = after.toString()
			}

			const result = await newsletterQuery(S_WHATSAPP_NET, 'get', [{ tag: 'messages', attrs }])
			return await parseFetchedMessages(result, 'messages', { decrypt: true })
		}

		// Legacy: (jid, count, since?, after?) => raw node
		const [jid, count, since, after] = args as [string, number, number?, number?]
		const messageUpdateAttrs: { count: string; since?: string; after?: string } = {
			count: count.toString()
		}
		if (typeof since === 'number') messageUpdateAttrs.since = since.toString()
		if (typeof after === 'number') messageUpdateAttrs.after = after.toString()

		const result = await query({
			tag: 'iq',
			attrs: {
				id: generateMessageTag(),
				type: 'get',
				xmlns: 'newsletter',
				to: jid
			},
			content: [
				{
					tag: 'message_updates',
					attrs: messageUpdateAttrs
				}
			]
		})
		return result
	}

	return {
		...sock,
		newsletterQuery,

		newsletterCreate: async (name: string, description?: string, picture?: WAMediaUpload): Promise<NewsletterMetadata> => {
			// Some accounts require accepting the ToS notice prior to creating a newsletter.
			try {
				await query({
					tag: 'iq',
					attrs: {
						to: S_WHATSAPP_NET,
						xmlns: 'tos',
						id: generateMessageTag(),
						type: 'set'
					},
					content: [
						{
							tag: 'notice',
							attrs: {
								id: '20601218',
								stage: '5'
							},
							content: []
						}
					]
				})
			} catch {
				// Best-effort only
			}

			const variables: any = {
				input: {
					name,
					description: description ?? null
				}
			}

			if (picture) {
				const { img } = await generateProfilePicture(picture)
				variables.input.picture = img.toString('base64')
			}

			const rawResponse = await executeWMexQuery<NewsletterCreateResponse>(
				variables,
				QueryIds.CREATE,
				XWAPaths.xwa2_newsletter_create
			)
			return parseNewsletterCreateResponse(rawResponse)
		},

		newsletterUpdate,

		newsletterSubscribers: async (jid: string) => {
			return executeWMexQuery<{ subscribers: number }>(
				{ newsletter_id: jid },
				QueryIds.SUBSCRIBERS,
				XWAPaths.xwa2_newsletter_subscribers
			)
		},

		newsletterMetadata: getNewsletterMetadata,

		newsletterFetchAllParticipating: async (viewRole?: NewsletterViewRole) => {
			const list = await executeWMexQuery<any[]>(
				{},
				QueryIds.SUBSCRIBED,
				XWAPaths.xwa2_newsletter_subscribed
			)

			const items = Array.isArray(list) ? list : []
			const data: Record<string, NewsletterMetadata> = {}

			const concurrency = 3
			let i = 0
			const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
				while (i < items.length) {
					const item = items[i++]
					const jid = item?.id
					if (!jid || !isJidNewsletter(jid)) continue
					const meta = await getNewsletterMetadata('jid', jid, viewRole)
					if (meta) data[meta.id] = meta
				}
			})
			await Promise.all(workers)

			return data
		},

		newsletterFollow: (jid: string) => {
			return executeWMexQuery({ newsletter_id: jid }, QueryIds.FOLLOW, XWAPaths.xwa2_newsletter_follow)
		},

		newsletterUnfollow: (jid: string) => {
			return executeWMexQuery({ newsletter_id: jid }, QueryIds.UNFOLLOW, XWAPaths.xwa2_newsletter_unfollow)
		},

		newsletterMute: (jid: string) => {
			return executeWMexQuery({ newsletter_id: jid }, QueryIds.MUTE, XWAPaths.xwa2_newsletter_mute_v2)
		},

		newsletterUnmute: (jid: string) => {
			return executeWMexQuery({ newsletter_id: jid }, QueryIds.UNMUTE, XWAPaths.xwa2_newsletter_unmute_v2)
		},

		newsletterUpdateName: async (jid: string, name: string) => {
			return await newsletterUpdate(jid, { name })
		},

		newsletterUpdateDescription: async (jid: string, description: string) => {
			return await newsletterUpdate(jid, { description })
		},

		newsletterUpdatePicture: async (jid: string, content: WAMediaUpload) => {
			const { img } = await generateProfilePicture(content)
			return await newsletterUpdate(jid, { picture: img.toString('base64') })
		},

		newsletterRemovePicture: async (jid: string) => {
			return await newsletterUpdate(jid, { picture: '' })
		},

		newsletterReactionMode: async (jid: string, mode: string) => {
			return await newsletterUpdate(jid, { settings: { reaction_codes: { value: mode } } })
		},

		newsletterReactMessage: async (jid: string, serverId: string, reaction?: string) => {
			await query({
				tag: 'message',
				attrs: {
					to: jid,
					...(reaction ? {} : { edit: '7' }),
					type: 'reaction',
					server_id: serverId,
					id: generateMessageTag()
				},
				content: [
					{
						tag: 'reaction',
						attrs: reaction ? { code: reaction } : {}
					}
				]
			})
		},

		newsletterFetchMessages,

		newsletterFetchUpdates: async (
			jid: string,
			count: number,
			{ since, after, decrypt }: { since?: number; after?: number; decrypt?: boolean } = {}
		) => {
			const attrs: Record<string, string> = { count: count.toString() }
			if (typeof since === 'number') attrs.since = since.toString()
			if (typeof after === 'number') attrs.after = after.toString()

			const result = await newsletterQuery(jid, 'get', [{ tag: 'message_updates', attrs }])
			return await parseFetchedMessages(result, 'updates', { decrypt: !!decrypt })
		},

		subscribeNewsletterUpdates: async (jid: string): Promise<{ duration: string } | null> => {
			const result = await query({
				tag: 'iq',
				attrs: {
					id: generateMessageTag(),
					type: 'set',
					xmlns: 'newsletter',
					to: jid
				},
				content: [{ tag: 'live_updates', attrs: {}, content: [] }]
			})
			const liveUpdatesNode = getBinaryNodeChild(result, 'live_updates')
			const duration = liveUpdatesNode?.attrs?.duration
			return duration ? { duration: duration } : null
		},

		newsletterAdminCount: async (jid: string): Promise<number> => {
			const response = await executeWMexQuery<{ admin_count: number }>(
				{ newsletter_id: jid },
				QueryIds.ADMIN_COUNT,
				XWAPaths.xwa2_newsletter_admin_count
			)
			return response.admin_count
		},

		newsletterChangeOwner: async (jid: string, newOwnerJid: string) => {
			await executeWMexQuery(
				{ newsletter_id: jid, user_id: newOwnerJid },
				QueryIds.CHANGE_OWNER,
				XWAPaths.xwa2_newsletter_change_owner
			)
		},

		newsletterDemote: async (jid: string, userJid: string) => {
			await executeWMexQuery({ newsletter_id: jid, user_id: userJid }, QueryIds.DEMOTE, XWAPaths.xwa2_newsletter_demote)
		},

		newsletterDelete: async (jid: string) => {
			await executeWMexQuery({ newsletter_id: jid }, QueryIds.DELETE, XWAPaths.xwa2_newsletter_delete_v2)
		}
	}
}

export type NewsletterSocket = ReturnType<typeof makeNewsletterSocket>
