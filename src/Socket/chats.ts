import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { createHash, randomBytes, randomUUID } from 'crypto'
import { parsePhoneNumber } from 'libphonenumber-js'
import { proto } from '../../WAProto/index.js'
import { DEFAULT_CACHE_TTLS, PROCESSABLE_HISTORY_TYPES } from '../Defaults'
import type {
	BotListInfo,
	CacheStore,
	ChatModification,
	ChatMutation,
	LTHashState,
	MessageUpsertType,
	PresenceData,
	SocketConfig,
	WABusinessHoursConfig,
	WABusinessProfile,
	WAMediaUpload,
	WAMessage,
	WAPatchCreate,
	WAPatchName,
	WAPresence,
	WAPrivacyCallValue,
	WAPrivacyGroupAddValue,
	WAPrivacyMessagesValue,
	WAPrivacyOnlineValue,
	WAPrivacyValue,
	WAReadReceiptsValue
} from '../Types'
import { ALL_WA_PATCH_NAMES } from '../Types'
import type { QuickReplyAction } from '../Types/Bussines.js'
import type { LabelActionBody } from '../Types/Label'
import { SyncState } from '../Types/State'
import {
	chatModificationToAppPatch,
	type ChatMutationMap,
	Curve,
	decodePatches,
	decodeSyncdSnapshot,
	encodeSyncdPatch,
	extractSyncdPatches,
	generateRegistrationId,
	generateProfilePicture,
	getHistoryMsg,
	md5,
	newLTHashState,
	processSyncAction
} from '../Utils'
import { makeMutex } from '../Utils/make-mutex'
import processMessage from '../Utils/process-message'
import { signedKeyPair } from '../Utils/crypto'
import { buildTcTokenFromJid } from '../Utils/tc-token-utils'
import {
	type BinaryNode,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	jidDecode,
	jidNormalizedUser,
	reduceBinaryNodeToDictionary,
	S_WHATSAPP_NET
} from '../WABinary'
import { USyncQuery, USyncUser } from '../WAUSync'
import { makeSocket } from './socket.js'
const MAX_SYNC_ATTEMPTS = 2

const DEFAULT_MOBILE_WA_VERSION = '2.26.5.76'
const MOBILE_WA_VERSION_CACHE_TTL_MS = 1000 * 60 * 60 * 12
let cachedMobileWAVersion: { value: string; fetchedAtMs: number } | undefined

type BanViolationInfo = {
	description: string
	duration: string
	risk: string
	status: string
	isPermanent?: boolean
}

const BAN_VIOLATION_INFO_BY_TYPE: Record<string, BanViolationInfo> = {
	'14': {
		description: 'Severe policy violation.',
		duration: 'Under review (6-24 hours)',
		risk: 'Very high',
		status: 'Pending internal review',
		isPermanent: false
	}
}

const normalizeMobileWAVersion = (version: string) => {
	const trimmed = version.trim()
	if (!/^\d+(\.\d+){1,3}$/.test(trimmed)) return undefined
	return trimmed.startsWith('2.') ? trimmed : `2.${trimmed}`
}

const fetchLatestMobileWAVersionFromApple = async () => {
	try {
		const resp = await fetch('https://itunes.apple.com/lookup?id=310633997&country=us', {
			headers: {
				'User-Agent': 'BailMod'
			}
		})
		if (!resp.ok) return undefined

		const json: any = await resp.json()
		const version = json?.results?.[0]?.version
		if (typeof version !== 'string') return undefined

		return normalizeMobileWAVersion(version)
	} catch {
		return undefined
	}
}

const getMobileWAVersion = async ({ forceRefresh }: { forceRefresh?: boolean } = {}) => {
	const envOverride =
		process.env.BAILMOD_MOBILE_WA_VERSION || process.env.WA_MOBILE_VERSION || process.env.WHATSAPP_MOBILE_VERSION
	if (envOverride) {
		const normalized = normalizeMobileWAVersion(envOverride)
		if (normalized) return normalized
	}

	const now = Date.now()
	if (
		!forceRefresh &&
		cachedMobileWAVersion &&
		now - cachedMobileWAVersion.fetchedAtMs < MOBILE_WA_VERSION_CACHE_TTL_MS
	) {
		return cachedMobileWAVersion.value
	}

	const latest = await fetchLatestMobileWAVersionFromApple()
	if (latest) {
		cachedMobileWAVersion = { value: latest, fetchedAtMs: now }
		return latest
	}

	return DEFAULT_MOBILE_WA_VERSION
}

export const makeChatsSocket = (config: SocketConfig) => {
	const {
		logger,
		markOnlineOnConnect,
		fireInitQueries,
		appStateMacVerification,
		shouldIgnoreJid,
		shouldSyncHistoryMessage,
		getMessage
	} = config
	const sock = makeSocket(config)
	const {
		ev,
		ws,
		authState,
		generateMessageTag,
		sendNode,
		query,
		signalRepository,
		onUnexpectedError,
		sendUnifiedSession
	} = sock

	let privacySettings: { [_: string]: string } | undefined

	let syncState: SyncState = SyncState.Connecting

	/** this mutex ensures that messages are processed in order */
	const messageMutex = makeMutex()

	/** this mutex ensures that receipts are processed in order */
	const receiptMutex = makeMutex()

	/** this mutex ensures that app state patches are processed in order */
	const appStatePatchMutex = makeMutex()

	/** this mutex ensures that notifications are processed in order */
	const notificationMutex = makeMutex()

	// Timeout for AwaitingInitialSync state
	let awaitingSyncTimeout: NodeJS.Timeout | undefined

	const placeholderResendCache =
		config.placeholderResendCache ||
		(new NodeCache<number>({
			stdTTL: DEFAULT_CACHE_TTLS.MSG_RETRY, // 1 hour
			useClones: false
		}) as CacheStore)

	if (!config.placeholderResendCache) {
		config.placeholderResendCache = placeholderResendCache
	}

	const checkWhatsAppNeedOfficialCache = new NodeCache<boolean>({
		stdTTL: 7 * 24 * 60 * 60,
		useClones: false
	})

	/** helper function to fetch the given app state sync key */
	const getAppStateSyncKey = async (keyId: string) => {
		const { [keyId]: key } = await authState.keys.get('app-state-sync-key', [keyId])
		return key
	}

	const fetchPrivacySettings = async (force = false) => {
		if (!privacySettings || force) {
			const { content } = await query({
				tag: 'iq',
				attrs: {
					xmlns: 'privacy',
					to: S_WHATSAPP_NET,
					type: 'get'
				},
				content: [{ tag: 'privacy', attrs: {} }]
			})
			privacySettings = reduceBinaryNodeToDictionary(content?.[0] as BinaryNode, 'category')
		}

		return privacySettings
	}

	/** helper function to run a privacy IQ query */
	const privacyQuery = async (name: string, value: string) => {
		await query({
			tag: 'iq',
			attrs: {
				xmlns: 'privacy',
				to: S_WHATSAPP_NET,
				type: 'set'
			},
			content: [
				{
					tag: 'privacy',
					attrs: {},
					content: [
						{
							tag: 'category',
							attrs: { name, value }
						}
					]
				}
			]
		})
	}

	const updateMessagesPrivacy = async (value: WAPrivacyMessagesValue) => {
		await privacyQuery('messages', value)
	}

	const updateCallPrivacy = async (value: WAPrivacyCallValue) => {
		await privacyQuery('calladd', value)
	}

	const updateLastSeenPrivacy = async (value: WAPrivacyValue) => {
		await privacyQuery('last', value)
	}

	const updateOnlinePrivacy = async (value: WAPrivacyOnlineValue) => {
		await privacyQuery('online', value)
	}

	const updateProfilePicturePrivacy = async (value: WAPrivacyValue) => {
		await privacyQuery('profile', value)
	}

	const updateStatusPrivacy = async (value: WAPrivacyValue) => {
		await privacyQuery('status', value)
	}

	const updateReadReceiptsPrivacy = async (value: WAReadReceiptsValue) => {
		await privacyQuery('readreceipts', value)
	}

	const updateGroupsAddPrivacy = async (value: WAPrivacyGroupAddValue) => {
		await privacyQuery('groupadd', value)
	}

	const checkWhatsApp = async (jid: string) => {
		if (!jid) {
			throw new Error('enter jid')
		}

		const resultData: {
			isBanned: boolean
			isNeedOfficialWa: boolean
			isPermanent: boolean
			number: string
			data?: {
				violation_type?: string
				violation_info?: Omit<BanViolationInfo, 'isPermanent'>
				in_app_ban_appeal?: any
				appeal_token?: string
				reason?: string
				retry_after?: number
				retry_after_human?: string
				ban_until?: string
				wa_version?: string
				raw?: any
				raw_voice?: any
			}
		} = {
			isBanned: false,
			isNeedOfficialWa: false,
			isPermanent: false,
			number: jid
		}

		let phoneNumber = jid
		const atIndex = phoneNumber.indexOf('@')
		if (atIndex !== -1) phoneNumber = phoneNumber.slice(0, atIndex)
		const colonIndex = phoneNumber.indexOf(':')
		if (colonIndex !== -1) phoneNumber = phoneNumber.slice(0, colonIndex)

		phoneNumber = phoneNumber.replace(/[^\d+]/g, '')

		if (phoneNumber.startsWith('00')) {
			phoneNumber = `+${phoneNumber.slice(2)}`
		} else if (!phoneNumber.startsWith('+') && phoneNumber.length > 0) {
			phoneNumber = `+${phoneNumber}`
		}

		let parsedNumber: ReturnType<typeof parsePhoneNumber>
		try {
			parsedNumber = parsePhoneNumber(phoneNumber)
		} catch (_err) {
			throw new Error('invalid jid/phone number')
		}

		const parsedE164 =
			typeof (parsedNumber as any)?.number === 'string' && (parsedNumber as any).number
				? (parsedNumber as any).number
				: phoneNumber
		resultData.number = parsedE164.replace(/[^\d]/g, '')

		const countryCode = parsedNumber.countryCallingCode
		const nationalNumber = parsedNumber.nationalNumber

		const urlencode = (str: string) => str.replace(/-/g, '%2d').replace(/_/g, '%5f').replace(/~/g, '%7e')

		const convertBufferToUrlHex = (buffer: Buffer) => {
			let id = ''
			buffer.forEach(x => {
				id += `%${x.toString(16).padStart(2, '0').toLowerCase()}`
			})
			return id
		}

		type MobileRegisterParams = {
			registrationId: number
			noiseKey: { public: Uint8Array }
			signedIdentityKey: { public: Uint8Array }
			signedPreKey: { keyId: number; keyPair: { public: Uint8Array }; signature: Uint8Array }
			deviceId: string
			phoneId: string
			identityId: Buffer
			backupToken: Buffer
			phoneNumberCountryCode: string
			phoneNumberNationalNumber: string
			phoneNumberMobileCountryCode: string
			phoneNumberMobileNetworkCode?: string
			method?: string
			captcha?: string
		}

		const buildMobileRegisterCode = (waVersion: string) => {
			const waVersionHash = createHash('md5').update(waVersion).digest('hex')
			const mobileToken = Buffer.from(`0a1mLfGUIBVrMKF1RdvLI5lkRBvof6vn0fD2QRSM${waVersionHash}`)
			const mobileUserAgent = `WhatsApp/${waVersion} iOS/17.5.1 Device/Apple-iPhone_13`
			const mobileRegistrationEndpoint = 'https://v.whatsapp.net/v2'

			const registrationParams = (params: MobileRegisterParams) => {
				const e_regid = Buffer.alloc(4)
				e_regid.writeInt32BE(params.registrationId)

				const phoneNumberCountryCode = params.phoneNumberCountryCode.replace('+', '').trim()
				const phoneNumberNationalNumber = params.phoneNumberNationalNumber.replace(/[/-\s)(]/g, '').trim()

				return {
					cc: phoneNumberCountryCode,
					in: phoneNumberNationalNumber,
					Rc: '0',
					lg: 'en',
					lc: 'GB',
					mistyped: '6',
					authkey: Buffer.from(params.noiseKey.public).toString('base64url'),
					e_regid: e_regid.toString('base64url'),
					e_keytype: 'BQ',
					e_ident: Buffer.from(params.signedIdentityKey.public).toString('base64url'),
					e_skey_id: 'AAAA',
					e_skey_val: Buffer.from(params.signedPreKey.keyPair.public).toString('base64url'),
					e_skey_sig: Buffer.from(params.signedPreKey.signature).toString('base64url'),
					fdid: params.phoneId,
					network_ratio_type: '1',
					expid: params.deviceId,
					simnum: '1',
					hasinrc: '1',
					pid: Math.floor(Math.random() * 1000).toString(),
					id: convertBufferToUrlHex(params.identityId),
					backup_token: convertBufferToUrlHex(params.backupToken),
					token: md5(Buffer.concat([mobileToken, Buffer.from(phoneNumberNationalNumber)])).toString('hex'),
					fraud_checkpoint_code: params.captcha
				}
			}

			const mobileRegisterFetch = async (
				path: string,
				opts: (RequestInit & { params?: Record<string, any> }) = {}
			) => {
				let url = `${mobileRegistrationEndpoint}${path}`
				if (opts.params) {
					const parameter: string[] = []
					for (const param in opts.params) {
						if (opts.params[param] !== null && opts.params[param] !== undefined) {
							parameter.push(param + '=' + urlencode(String(opts.params[param])))
						}
					}

					url += `?${parameter.join('&')}`
					delete (opts as any).params
				}

				const headers = new Headers(opts.headers || {})
				headers.set('User-Agent', mobileUserAgent)

				const { body: _body, method: _method, ...rest } = opts as RequestInit
				const response = await fetch(url, { ...rest, method: 'GET', headers })
				let json: any
				try {
					json = await response.json()
				} catch (_err) {
					throw { reason: 'invalid_response', status: response.status }
				}

				if (response.status > 300 || json?.reason) {
					throw json
				}

				if (json?.status && !['ok', 'sent'].includes(json.status)) {
					throw json
				}

				return json
			}

			const mobileRegisterCode = (params: MobileRegisterParams) => {
				return mobileRegisterFetch('/code', {
					...config.options,
					params: {
						...registrationParams(params),
						mcc: `${params.phoneNumberMobileCountryCode}`.padStart(3, '0'),
						mnc: `${params.phoneNumberMobileNetworkCode || '001'}`.padStart(3, '0'),
						sim_mcc: '000',
						sim_mnc: '000',
						method: params.method || 'sms',
						reason: '',
						hasav: '1'
					}
				})
			}

			return { mobileRegisterCode, waVersion }
		}

		const makeMobileParams = (method: 'sms' | 'voice'): MobileRegisterParams => {
			const identityKey = Curve.generateKeyPair()
			const uuidHex = randomUUID().replace(/-/g, '')
			const deviceId = Buffer.from(uuidHex, 'hex').toString('base64url')

			return {
				registrationId: generateRegistrationId(),
				noiseKey: Curve.generateKeyPair(),
				signedIdentityKey: identityKey,
				signedPreKey: signedKeyPair(identityKey, 1),
				deviceId,
				phoneId: randomUUID(),
				identityId: randomBytes(20),
				backupToken: randomBytes(20),
				phoneNumberCountryCode: `+${countryCode}`,
				phoneNumberNationalNumber: nationalNumber,
				phoneNumberMobileCountryCode: '510',
				phoneNumberMobileNetworkCode: '10',
				method
			}
		}

		const attemptRegisterCode = (waVersion: string, method: 'sms' | 'voice') => {
			const { mobileRegisterCode } = buildMobileRegisterCode(waVersion)
			return mobileRegisterCode(makeMobileParams(method))
		}

		const formatDurationSeconds = (seconds: number) => {
			const sec = Math.max(0, Math.floor(seconds))
			const days = Math.floor(sec / 86400)
			const hours = Math.floor((sec % 86400) / 3600)
			const mins = Math.floor((sec % 3600) / 60)
			const parts: string[] = []
			if (days) parts.push(`${days}d`)
			if (hours) parts.push(`${hours}h`)
			if (mins) parts.push(`${mins}m`)
			if (!parts.length) parts.push(`${sec}s`)
			return parts.join(' ')
		}

		const stripNullishDeep = (value: any): any => {
			if (value === null || value === undefined) return undefined
			if (Array.isArray(value)) {
				const mapped = value.map(stripNullishDeep).filter(v => v !== undefined)
				return mapped
			}
			if (typeof value === 'object') {
				const out: any = {}
				for (const [k, v] of Object.entries(value)) {
					const cleaned = stripNullishDeep(v)
					if (cleaned !== undefined) out[k] = cleaned
				}
				return out
			}
			return value
		}

		const cacheKey = resultData.number || parsedE164
		const cachedNeedOfficial = checkWhatsAppNeedOfficialCache.get(cacheKey) === true

		let waVersion = await getMobileWAVersion()
		let smsResp: any
		let smsErr: any
		let voiceResp: any
		let voiceErr: any

		const runAttempt = async (method: 'sms' | 'voice') => {
			try {
				const resp = await attemptRegisterCode(waVersion, method)
				return { ok: true as const, resp }
			} catch (err: any) {
				return { ok: false as const, err }
			}
		}

		const sms1 = await runAttempt('sms')
		if (sms1.ok) {
			smsResp = sms1.resp
		} else {
			smsErr = sms1.err
		}

		const smsReason1 = typeof smsErr?.reason === 'string' ? smsErr.reason : undefined
		if (!smsResp && smsReason1?.toLowerCase() === 'old_version') {
			const refreshed = await getMobileWAVersion({ forceRefresh: true })
			if (refreshed && refreshed !== waVersion) {
				waVersion = refreshed
				const sms2 = await runAttempt('sms')
				if (sms2.ok) {
					smsResp = sms2.resp
					smsErr = undefined
				} else {
					smsErr = sms2.err
				}
			}
		}

		const smsReasonLc = typeof smsErr?.reason === 'string' ? smsErr.reason.toLowerCase() : undefined
		const shouldTryVoice =
			!smsResp &&
			(smsReasonLc === 'too_recent' ||
				smsReasonLc === 'temporarily_unavailable' ||
				smsReasonLc === 'rate_limit' ||
				smsReasonLc === 'too_many' ||
				smsReasonLc === 'too_many_requests' ||
				smsReasonLc === 'too_many_attempts' ||
				smsReasonLc === 'too_many_guesses' ||
				smsReasonLc === 'no_routes')

		if (shouldTryVoice) {
			const voice1 = await runAttempt('voice')
			if (voice1.ok) {
				voiceResp = voice1.resp
			} else {
				voiceErr = voice1.err
			}
		}

		const smsReason = typeof smsErr?.reason === 'string' ? smsErr.reason : undefined
		const smsReasonLc2 = smsReason?.toLowerCase()
		const voiceReason = typeof voiceErr?.reason === 'string' ? voiceErr.reason : undefined
		const voiceReasonLc = voiceReason?.toLowerCase()

		const needsOfficialSignal =
			!!smsErr?.custom_block_screen ||
			!!voiceErr?.custom_block_screen ||
			smsReasonLc2 === 'no_routes' ||
			voiceReasonLc === 'no_routes' ||
			(smsReasonLc2 ? smsReasonLc2.includes('support') : false) ||
			(voiceReasonLc ? voiceReasonLc.includes('support') : false) ||
			(smsReasonLc2 ? smsReasonLc2.includes('official') || smsReasonLc2.includes('unofficial') || smsReasonLc2.includes('mod') : false) ||
			(voiceReasonLc ? voiceReasonLc.includes('official') || voiceReasonLc.includes('unofficial') || voiceReasonLc.includes('mod') : false)

		if (needsOfficialSignal) {
			checkWhatsAppNeedOfficialCache.set(cacheKey, true)
		}

		resultData.isNeedOfficialWa = cachedNeedOfficial || needsOfficialSignal

		const hasAppealToken =
			typeof smsErr?.appeal_token === 'string' ||
			typeof voiceErr?.appeal_token === 'string'

		const hasBanFields =
			(smsErr?.violation_type !== null && smsErr?.violation_type !== undefined) ||
			(voiceErr?.violation_type !== null && voiceErr?.violation_type !== undefined) ||
			(smsErr?.in_app_ban_appeal !== null && smsErr?.in_app_ban_appeal !== undefined) ||
			(voiceErr?.in_app_ban_appeal !== null && voiceErr?.in_app_ban_appeal !== undefined)

		const looksLikeBanReason = (r?: string) => (r ? /(^|_)ban(ned)?($|_)/.test(r) : false)

		const isBannedSignal =
			smsReasonLc2 === 'blocked' ||
			voiceReasonLc === 'blocked' ||
			hasAppealToken ||
			hasBanFields ||
			looksLikeBanReason(smsReasonLc2) ||
			looksLikeBanReason(voiceReasonLc)

		resultData.isBanned = isBannedSignal

		const scoreErr = (e: any) => {
			if (!e) return -1
			const r = typeof e.reason === 'string' ? e.reason.toLowerCase() : ''
			let score = 0
			if (r === 'blocked') score += 100
			if (typeof e.appeal_token === 'string') score += 95
			if (e.violation_type !== null && e.violation_type !== undefined) score += 90
			if (e.in_app_ban_appeal !== null && e.in_app_ban_appeal !== undefined) score += 85
			if (e.custom_block_screen) score += 70
			if (r === 'no_routes') score += 65
			if (r.includes('support')) score += 60
			if (r.includes('official') || r.includes('unofficial') || r.includes('mod')) score += 55
			if (typeof e.retry_after === 'number' || (typeof e.retry_after === 'string' && /^\d+$/.test(e.retry_after))) {
				score += 30
			}
			if (typeof e.reason === 'string') score += 5
			return score
		}

		const primaryErr = scoreErr(smsErr) >= scoreErr(voiceErr) ? smsErr : voiceErr
		const secondaryErr = primaryErr === smsErr ? voiceErr : smsErr

		const reason = typeof primaryErr?.reason === 'string' ? primaryErr.reason : undefined
		const retryAfter =
			typeof primaryErr?.retry_after === 'number'
				? primaryErr.retry_after
				: typeof primaryErr?.retry_after === 'string' && /^\d+$/.test(primaryErr.retry_after)
					? Number(primaryErr.retry_after)
					: undefined

		const violationType =
			primaryErr?.violation_type !== null && primaryErr?.violation_type !== undefined
				? String(primaryErr.violation_type)
				: secondaryErr?.violation_type !== null && secondaryErr?.violation_type !== undefined
					? String(secondaryErr.violation_type)
					: undefined

		const inAppBanAppeal =
			primaryErr?.in_app_ban_appeal !== null && primaryErr?.in_app_ban_appeal !== undefined
				? primaryErr.in_app_ban_appeal
				: secondaryErr?.in_app_ban_appeal !== null && secondaryErr?.in_app_ban_appeal !== undefined
					? secondaryErr.in_app_ban_appeal
					: undefined

		const appealToken =
			typeof primaryErr?.appeal_token === 'string'
				? primaryErr.appeal_token
				: typeof secondaryErr?.appeal_token === 'string'
					? secondaryErr.appeal_token
					: undefined

		const waVersionFromResponse =
			typeof primaryErr?.wa_version === 'string'
				? primaryErr.wa_version
				: typeof secondaryErr?.wa_version === 'string'
					? secondaryErr.wa_version
					: undefined

		const violationInfo = violationType ? BAN_VIOLATION_INFO_BY_TYPE[violationType] : undefined
		const isPermanent =
			resultData.isBanned &&
			retryAfter === undefined &&
			(violationInfo?.isPermanent !== undefined ? violationInfo.isPermanent : true)
		resultData.isPermanent = isPermanent

		const data: NonNullable<typeof resultData.data> = { wa_version: waVersionFromResponse || waVersion }

		if (reason) data.reason = reason
		if (retryAfter !== undefined) {
			data.retry_after = retryAfter
			data.retry_after_human = formatDurationSeconds(retryAfter)
			data.ban_until = new Date(Date.now() + retryAfter * 1000).toISOString()
		}

		if (violationType) data.violation_type = violationType
		if (violationInfo) {
			const { isPermanent: _ignore, ...rest } = violationInfo
			data.violation_info = rest
		}
		if (inAppBanAppeal !== undefined) data.in_app_ban_appeal = inAppBanAppeal
		if (appealToken) data.appeal_token = appealToken
		if (smsErr || smsResp) data.raw = stripNullishDeep(smsErr || smsResp)
		if (voiceErr || voiceResp) data.raw_voice = stripNullishDeep(voiceErr || voiceResp)

		resultData.data = data

		return JSON.stringify(resultData, null, 2)
	}

	const updateDefaultDisappearingMode = async (duration: number) => {
		await query({
			tag: 'iq',
			attrs: {
				xmlns: 'disappearing_mode',
				to: S_WHATSAPP_NET,
				type: 'set'
			},
			content: [
				{
					tag: 'disappearing_mode',
					attrs: {
						duration: duration.toString()
					}
				}
			]
		})
	}

	const getBotListV2 = async () => {
		const resp = await query({
			tag: 'iq',
			attrs: {
				xmlns: 'bot',
				to: S_WHATSAPP_NET,
				type: 'get'
			},
			content: [
				{
					tag: 'bot',
					attrs: {
						v: '2'
					}
				}
			]
		})

		const botNode = getBinaryNodeChild(resp, 'bot')

		const botList: BotListInfo[] = []
		for (const section of getBinaryNodeChildren(botNode, 'section')) {
			if (section.attrs.type === 'all') {
				for (const bot of getBinaryNodeChildren(section, 'bot')) {
					botList.push({
						jid: bot.attrs.jid!,
						personaId: bot.attrs['persona_id']!
					})
				}
			}
		}

		return botList
	}

	const fetchStatus = async (...jids: string[]) => {
		const usyncQuery = new USyncQuery().withStatusProtocol()

		for (const jid of jids) {
			usyncQuery.withUser(new USyncUser().withId(jid))
		}

		const result = await sock.executeUSyncQuery(usyncQuery)
		if (result) {
			return result.list
		}
	}

	const fetchDisappearingDuration = async (...jids: string[]) => {
		const usyncQuery = new USyncQuery().withDisappearingModeProtocol()

		for (const jid of jids) {
			usyncQuery.withUser(new USyncUser().withId(jid))
		}

		const result = await sock.executeUSyncQuery(usyncQuery)
		if (result) {
			return result.list
		}
	}

	/** update the profile picture for yourself or a group */
	const updateProfilePicture = async (
		jid: string,
		content: WAMediaUpload,
		dimensions?: { width: number; height: number }
	) => {
		let targetJid
		if (!jid) {
			throw new Boom(
				'Illegal no-jid profile update. Please specify either your ID or the ID of the chat you wish to update'
			)
		}

		if (jidNormalizedUser(jid) !== jidNormalizedUser(authState.creds.me!.id)) {
			targetJid = jidNormalizedUser(jid) // in case it is someone other than us
		} else {
			targetJid = undefined
		}

		const { img } = await generateProfilePicture(content, dimensions)
		await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'w:profile:picture',
				...(targetJid ? { target: targetJid } : {})
			},
			content: [
				{
					tag: 'picture',
					attrs: { type: 'image' },
					content: img
				}
			]
		})
	}

	/** remove the profile picture for yourself or a group */
	const removeProfilePicture = async (jid: string) => {
		let targetJid
		if (!jid) {
			throw new Boom(
				'Illegal no-jid profile update. Please specify either your ID or the ID of the chat you wish to update'
			)
		}

		if (jidNormalizedUser(jid) !== jidNormalizedUser(authState.creds.me!.id)) {
			targetJid = jidNormalizedUser(jid) // in case it is someone other than us
		} else {
			targetJid = undefined
		}

		await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'w:profile:picture',
				...(targetJid ? { target: targetJid } : {})
			}
		})
	}

	/** update the profile status for yourself */
	const updateProfileStatus = async (status: string) => {
		await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'status'
			},
			content: [
				{
					tag: 'status',
					attrs: {},
					content: Buffer.from(status, 'utf-8')
				}
			]
		})
	}

	const updateProfileName = async (name: string) => {
		await chatModify({ pushNameSetting: name }, '')
	}

	const fetchBlocklist = async () => {
		const result = await query({
			tag: 'iq',
			attrs: {
				xmlns: 'blocklist',
				to: S_WHATSAPP_NET,
				type: 'get'
			}
		})

		const listNode = getBinaryNodeChild(result, 'list')
		return getBinaryNodeChildren(listNode, 'item').map(n => n.attrs.jid)
	}

	const updateBlockStatus = async (jid: string, action: 'block' | 'unblock') => {
		await query({
			tag: 'iq',
			attrs: {
				xmlns: 'blocklist',
				to: S_WHATSAPP_NET,
				type: 'set'
			},
			content: [
				{
					tag: 'item',
					attrs: {
						action,
						jid
					}
				}
			]
		})
	}

	const getBusinessProfile = async (jid: string): Promise<WABusinessProfile | void> => {
		const results = await query({
			tag: 'iq',
			attrs: {
				to: 's.whatsapp.net',
				xmlns: 'w:biz',
				type: 'get'
			},
			content: [
				{
					tag: 'business_profile',
					attrs: { v: '244' },
					content: [
						{
							tag: 'profile',
							attrs: { jid }
						}
					]
				}
			]
		})

		const profileNode = getBinaryNodeChild(results, 'business_profile')
		const profiles = getBinaryNodeChild(profileNode, 'profile')
		if (profiles) {
			const address = getBinaryNodeChild(profiles, 'address')
			const description = getBinaryNodeChild(profiles, 'description')
			const website = getBinaryNodeChild(profiles, 'website')
			const email = getBinaryNodeChild(profiles, 'email')
			const category = getBinaryNodeChild(getBinaryNodeChild(profiles, 'categories'), 'category')
			const businessHours = getBinaryNodeChild(profiles, 'business_hours')
			const businessHoursConfig = businessHours
				? getBinaryNodeChildren(businessHours, 'business_hours_config')
				: undefined
			const websiteStr = website?.content?.toString()
			return {
				wid: profiles.attrs?.jid,
				address: address?.content?.toString(),
				description: description?.content?.toString() || '',
				website: websiteStr ? [websiteStr] : [],
				email: email?.content?.toString(),
				category: category?.content?.toString(),
				business_hours: {
					timezone: businessHours?.attrs?.timezone,
					business_config: businessHoursConfig?.map(({ attrs }) => attrs as unknown as WABusinessHoursConfig)
				}
			}
		}
	}

	const cleanDirtyBits = async (type: 'account_sync' | 'groups', fromTimestamp?: number | string) => {
		logger.info({ fromTimestamp }, 'clean dirty bits ' + type)
		await sendNode({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'urn:xmpp:whatsapp:dirty',
				id: generateMessageTag()
			},
			content: [
				{
					tag: 'clean',
					attrs: {
						type,
						...(fromTimestamp ? { timestamp: fromTimestamp.toString() } : null)
					}
				}
			]
		})
	}

	const newAppStateChunkHandler = (isInitialSync: boolean) => {
		return {
			onMutation(mutation: ChatMutation) {
				processSyncAction(
					mutation,
					ev,
					authState.creds.me!,
					isInitialSync ? { accountSettings: authState.creds.accountSettings } : undefined,
					logger
				)
			}
		}
	}

	const resyncAppState = ev.createBufferedFunction(
		async (collections: readonly WAPatchName[], isInitialSync: boolean) => {
			// we use this to determine which events to fire
			// otherwise when we resync from scratch -- all notifications will fire
			const initialVersionMap: { [T in WAPatchName]?: number } = {}
			const globalMutationMap: ChatMutationMap = {}

			await authState.keys.transaction(async () => {
				const collectionsToHandle = new Set<string>(collections)
				// in case something goes wrong -- ensure we don't enter a loop that cannot be exited from
				const attemptsMap: { [T in WAPatchName]?: number } = {}
				// keep executing till all collections are done
				// sometimes a single patch request will not return all the patches (God knows why)
				// so we fetch till they're all done (this is determined by the "has_more_patches" flag)
				while (collectionsToHandle.size) {
					const states = {} as { [T in WAPatchName]: LTHashState }
					const nodes: BinaryNode[] = []

					for (const name of collectionsToHandle as Set<WAPatchName>) {
						const result = await authState.keys.get('app-state-sync-version', [name])
						let state = result[name]

						if (state) {
							if (typeof initialVersionMap[name] === 'undefined') {
								initialVersionMap[name] = state.version
							}
						} else {
							state = newLTHashState()
						}

						states[name] = state

						logger.info(`resyncing ${name} from v${state.version}`)

						nodes.push({
							tag: 'collection',
							attrs: {
								name,
								version: state.version.toString(),
								// return snapshot if being synced from scratch
								return_snapshot: (!state.version).toString()
							}
						})
					}

					const result = await query({
						tag: 'iq',
						attrs: {
							to: S_WHATSAPP_NET,
							xmlns: 'w:sync:app:state',
							type: 'set'
						},
						content: [
							{
								tag: 'sync',
								attrs: {},
								content: nodes
							}
						]
					})

					// extract from binary node
					const decoded = await extractSyncdPatches(result, config?.options)
					for (const key in decoded) {
						const name = key as WAPatchName
						const { patches, hasMorePatches, snapshot } = decoded[name]
						try {
							if (snapshot) {
								const { state: newState, mutationMap } = await decodeSyncdSnapshot(
									name,
									snapshot,
									getAppStateSyncKey,
									initialVersionMap[name],
									appStateMacVerification.snapshot
								)
								states[name] = newState
								Object.assign(globalMutationMap, mutationMap)

								logger.info(`restored state of ${name} from snapshot to v${newState.version} with mutations`)

								await authState.keys.set({ 'app-state-sync-version': { [name]: newState } })
							}

							// only process if there are syncd patches
							if (patches.length) {
								const { state: newState, mutationMap } = await decodePatches(
									name,
									patches,
									states[name],
									getAppStateSyncKey,
									config.options,
									initialVersionMap[name],
									logger,
									appStateMacVerification.patch
								)

								await authState.keys.set({ 'app-state-sync-version': { [name]: newState } })

								logger.info(`synced ${name} to v${newState.version}`)
								initialVersionMap[name] = newState.version

								Object.assign(globalMutationMap, mutationMap)
							}

							if (hasMorePatches) {
								logger.info(`${name} has more patches...`)
							} else {
								// collection is done with sync
								collectionsToHandle.delete(name)
							}
						} catch (error: any) {
							// if retry attempts overshoot
							// or key not found
							const isIrrecoverableError =
								attemptsMap[name]! >= MAX_SYNC_ATTEMPTS ||
								error.output?.statusCode === 404 ||
								error.name === 'TypeError'
							logger.info(
								{ name, error: error.stack },
								`failed to sync state from version${isIrrecoverableError ? '' : ', removing and trying from scratch'}`
							)
							await authState.keys.set({ 'app-state-sync-version': { [name]: null } })
							// increment number of retries
							attemptsMap[name] = (attemptsMap[name] || 0) + 1

							if (isIrrecoverableError) {
								// stop retrying
								collectionsToHandle.delete(name)
							}
						}
					}
				}
			}, authState?.creds?.me?.id || 'resync-app-state')

			const { onMutation } = newAppStateChunkHandler(isInitialSync)
			for (const key in globalMutationMap) {
				onMutation(globalMutationMap[key]!)
			}
		}
	)

	/**
	 * fetch the profile picture of a user/group
	 * type = "preview" for a low res picture
	 * type = "image for the high res picture"
	 */
	const profilePictureUrl = async (jid: string, type: 'preview' | 'image' = 'preview', timeoutMs?: number) => {
		const baseContent: BinaryNode[] = [{ tag: 'picture', attrs: { type, query: 'url' } }]

		const tcTokenContent = await buildTcTokenFromJid({ authState, jid, baseContent })

		jid = jidNormalizedUser(jid)
		const result = await query(
			{
				tag: 'iq',
				attrs: {
					target: jid,
					to: S_WHATSAPP_NET,
					type: 'get',
					xmlns: 'w:profile:picture'
				},
				content: tcTokenContent
			},
			timeoutMs
		)
		const child = getBinaryNodeChild(result, 'picture')
		return child?.attrs?.url
	}

	const createCallLink = async (type: 'audio' | 'video', event?: { startTime: number }, timeoutMs?: number) => {
		const result = await query(
			{
				tag: 'call',
				attrs: {
					id: generateMessageTag(),
					to: '@call'
				},
				content: [
					{
						tag: 'link_create',
						attrs: { media: type },
						content: event ? [{ tag: 'event', attrs: { start_time: String(event.startTime) } }] : undefined
					}
				]
			},
			timeoutMs
		)
		const child = getBinaryNodeChild(result, 'link_create')
		return child?.attrs?.token
	}

	const sendPresenceUpdate = async (type: WAPresence, toJid?: string) => {
		const me = authState.creds.me!
		const isAvailableType = type === 'available'
		if (isAvailableType || type === 'unavailable') {
			if (!me.name) {
				logger.warn('no name present, ignoring presence update request...')
				return
			}

			ev.emit('connection.update', { isOnline: isAvailableType })

			if (isAvailableType) {
				void sendUnifiedSession()
			}

			await sendNode({
				tag: 'presence',
				attrs: {
					name: me.name.replace(/@/g, ''),
					type
				}
			})
		} else {
			const { server } = jidDecode(toJid)!
			const isLid = server === 'lid'

			await sendNode({
				tag: 'chatstate',
				attrs: {
					from: isLid ? me.lid! : me.id,
					to: toJid!
				},
				content: [
					{
						tag: type === 'recording' ? 'composing' : type,
						attrs: type === 'recording' ? { media: 'audio' } : {}
					}
				]
			})
		}
	}

	/**
	 * @param toJid the jid to subscribe to
	 * @param tcToken token for subscription, use if present
	 */
	const presenceSubscribe = async (toJid: string) => {
		const tcTokenContent = await buildTcTokenFromJid({ authState, jid: toJid })

		return sendNode({
			tag: 'presence',
			attrs: {
				to: toJid,
				id: generateMessageTag(),
				type: 'subscribe'
			},
			content: tcTokenContent
		})
	}

	const handlePresenceUpdate = ({ tag, attrs, content }: BinaryNode) => {
		let presence: PresenceData | undefined
		const jid = attrs.from
		const participant = attrs.participant || attrs.from

		if (shouldIgnoreJid(jid!) && jid !== S_WHATSAPP_NET) {
			return
		}

		if (tag === 'presence') {
			presence = {
				lastKnownPresence: attrs.type === 'unavailable' ? 'unavailable' : 'available',
				lastSeen: attrs.last && attrs.last !== 'deny' ? +attrs.last : undefined
			}
		} else if (Array.isArray(content)) {
			const [firstChild] = content
			let type = firstChild!.tag as WAPresence
			if (type === 'paused') {
				type = 'available'
			}

			if (firstChild!.attrs?.media === 'audio') {
				type = 'recording'
			}

			presence = { lastKnownPresence: type }
		} else {
			logger.error({ tag, attrs, content }, 'recv invalid presence node')
		}

		if (presence) {
			ev.emit('presence.update', { id: jid!, presences: { [participant!]: presence } })
		}
	}

	const appPatch = async (patchCreate: WAPatchCreate) => {
		const name = patchCreate.type
		const myAppStateKeyId = authState.creds.myAppStateKeyId
		if (!myAppStateKeyId) {
			throw new Boom('App state key not present!', { statusCode: 400 })
		}

		let initial: LTHashState
		let encodeResult: { patch: proto.ISyncdPatch; state: LTHashState }

		await appStatePatchMutex.mutex(async () => {
			await authState.keys.transaction(async () => {
				logger.debug({ patch: patchCreate }, 'applying app patch')

				await resyncAppState([name], false)

				const { [name]: currentSyncVersion } = await authState.keys.get('app-state-sync-version', [name])
				initial = currentSyncVersion || newLTHashState()

				encodeResult = await encodeSyncdPatch(patchCreate, myAppStateKeyId, initial, getAppStateSyncKey)
				const { patch, state } = encodeResult

				const node: BinaryNode = {
					tag: 'iq',
					attrs: {
						to: S_WHATSAPP_NET,
						type: 'set',
						xmlns: 'w:sync:app:state'
					},
					content: [
						{
							tag: 'sync',
							attrs: {},
							content: [
								{
									tag: 'collection',
									attrs: {
										name,
										version: (state.version - 1).toString(),
										return_snapshot: 'false'
									},
									content: [
										{
											tag: 'patch',
											attrs: {},
											content: proto.SyncdPatch.encode(patch).finish()
										}
									]
								}
							]
						}
					]
				}
				await query(node)

				await authState.keys.set({ 'app-state-sync-version': { [name]: state } })
			}, authState?.creds?.me?.id || 'app-patch')
		})

		if (config.emitOwnEvents) {
			const { onMutation } = newAppStateChunkHandler(false)
			const { mutationMap } = await decodePatches(
				name,
				[{ ...encodeResult!.patch, version: { version: encodeResult!.state.version } }],
				initial!,
				getAppStateSyncKey,
				config.options,
				undefined,
				logger
			)
			for (const key in mutationMap) {
				onMutation(mutationMap[key]!)
			}
		}
	}

	/** sending non-abt props may fix QR scan fail if server expects */
	const fetchProps = async () => {
		//TODO: implement both protocol 1 and protocol 2 prop fetching, specially for abKey for WM
		const resultNode = await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				xmlns: 'w',
				type: 'get'
			},
			content: [
				{
					tag: 'props',
					attrs: {
						protocol: '2',
						hash: authState?.creds?.lastPropHash || ''
					}
				}
			]
		})

		const propsNode = getBinaryNodeChild(resultNode, 'props')

		let props: { [_: string]: string } = {}
		if (propsNode) {
			if (propsNode.attrs?.hash) {
				// on some clients, the hash is returning as undefined
				authState.creds.lastPropHash = propsNode?.attrs?.hash
				ev.emit('creds.update', authState.creds)
			}

			props = reduceBinaryNodeToDictionary(propsNode, 'prop')
		}

		logger.debug('fetched props')

		return props
	}

	/**
	 * modify a chat -- mark unread, read etc.
	 * lastMessages must be sorted in reverse chronologically
	 * requires the last messages till the last message received; required for archive & unread
	 */
	const chatModify = (mod: ChatModification, jid: string) => {
		const patch = chatModificationToAppPatch(mod, jid)
		return appPatch(patch)
	}

	/**
	 * Enable/Disable link preview privacy, not related to baileys link preview generation
	 */
	const updateDisableLinkPreviewsPrivacy = (isPreviewsDisabled: boolean) => {
		return chatModify(
			{
				disableLinkPreviews: { isPreviewsDisabled }
			},
			''
		)
	}

	/**
	 * Star or Unstar a message
	 */
	const star = (jid: string, messages: { id: string; fromMe?: boolean }[], star: boolean) => {
		return chatModify(
			{
				star: {
					messages,
					star
				}
			},
			jid
		)
	}

	/**
	 * Add or Edit Contact
	 */
	const addOrEditContact = (jid: string, contact: proto.SyncActionValue.IContactAction) => {
		return chatModify(
			{
				contact
			},
			jid
		)
	}

	/**
	 * Remove Contact
	 */
	const removeContact = (jid: string) => {
		return chatModify(
			{
				contact: null
			},
			jid
		)
	}

	/**
	 * Adds label
	 */
	const addLabel = (jid: string, labels: LabelActionBody) => {
		return chatModify(
			{
				addLabel: {
					...labels
				}
			},
			jid
		)
	}

	/**
	 * Adds label for the chats
	 */
	const addChatLabel = (jid: string, labelId: string) => {
		return chatModify(
			{
				addChatLabel: {
					labelId
				}
			},
			jid
		)
	}

	/**
	 * Removes label for the chat
	 */
	const removeChatLabel = (jid: string, labelId: string) => {
		return chatModify(
			{
				removeChatLabel: {
					labelId
				}
			},
			jid
		)
	}

	/**
	 * Adds label for the message
	 */
	const addMessageLabel = (jid: string, messageId: string, labelId: string) => {
		return chatModify(
			{
				addMessageLabel: {
					messageId,
					labelId
				}
			},
			jid
		)
	}

	/**
	 * Removes label for the message
	 */
	const removeMessageLabel = (jid: string, messageId: string, labelId: string) => {
		return chatModify(
			{
				removeMessageLabel: {
					messageId,
					labelId
				}
			},
			jid
		)
	}

	/**
	 * Add or Edit Quick Reply
	 */
	const addOrEditQuickReply = (quickReply: QuickReplyAction) => {
		return chatModify(
			{
				quickReply
			},
			''
		)
	}

	/**
	 * Remove Quick Reply
	 */
	const removeQuickReply = (timestamp: string) => {
		return chatModify(
			{
				quickReply: { timestamp, deleted: true }
			},
			''
		)
	}

	/**
	 * queries need to be fired on connection open
	 * help ensure parity with WA Web
	 * */
	const executeInitQueries = async () => {
		await Promise.all([fetchProps(), fetchBlocklist(), fetchPrivacySettings()])
	}

	const upsertMessage = ev.createBufferedFunction(async (msg: WAMessage, type: MessageUpsertType) => {
		ev.emit('messages.upsert', { messages: [msg], type })

		const runPushNameSideEffects = async () => {
			if (!msg.pushName) {
				return
			}
			let jid = msg.key.fromMe ? authState.creds.me!.id : msg.key.participant || msg.key.remoteJid
			jid = jidNormalizedUser(jid!)

			if (!msg.key.fromMe) {
				ev.emit('contacts.update', [{ id: jid, notify: msg.pushName, verifiedName: msg.verifiedBizName! }])
			}

			// update our pushname too
			if (msg.key.fromMe && msg.pushName && authState.creds.me?.name !== msg.pushName) {
				ev.emit('creds.update', { me: { ...authState.creds.me!, name: msg.pushName } })
			}
		}

		if (type === 'notify') {
			void runPushNameSideEffects().catch(err => {
				logger.warn({ err }, 'background pushName processing failed')
			})
		} else {
			await runPushNameSideEffects()
		}

		const historyMsg = getHistoryMsg(msg.message!)
		const shouldProcessHistoryMsg = historyMsg
			? shouldSyncHistoryMessage(historyMsg) &&
				PROCESSABLE_HISTORY_TYPES.includes(historyMsg.syncType! as proto.HistorySync.HistorySyncType)
			: false

		// State machine: decide on sync and flush
		if (historyMsg && syncState === SyncState.AwaitingInitialSync) {
			if (awaitingSyncTimeout) {
				clearTimeout(awaitingSyncTimeout)
				awaitingSyncTimeout = undefined
			}

			if (shouldProcessHistoryMsg) {
				syncState = SyncState.Syncing
				logger.info('Transitioned to Syncing state')
				// Let doAppStateSync handle the final flush after it's done
			} else {
				syncState = SyncState.Online
				logger.info('History sync skipped, transitioning to Online state and flushing buffer')
				ev.flush()
			}
		}

		const doAppStateSync = async () => {
			if (syncState === SyncState.Syncing) {
				logger.info('Doing app state sync')
				await resyncAppState(ALL_WA_PATCH_NAMES, true)

				// Sync is complete, go online and flush everything
				syncState = SyncState.Online
				logger.info('App state sync complete, transitioning to Online state and flushing buffer')
				ev.flush()

				const accountSyncCounter = (authState.creds.accountSyncCounter || 0) + 1
				ev.emit('creds.update', { accountSyncCounter })
			}
		}

		const runProcessing = async () => {
			await Promise.all([
				(async () => {
					if (shouldProcessHistoryMsg) {
						await doAppStateSync()
					}
				})(),
				processMessage(msg, {
					signalRepository,
					shouldProcessHistoryMsg,
					placeholderResendCache,
					ev,
					creds: authState.creds,
					keyStore: authState.keys,
					logger,
					options: config.options,
					getMessage
				})
			])

			// If the app state key arrives and we are waiting to sync, trigger the sync now.
			if (msg.message?.protocolMessage?.appStateSyncKeyShare && syncState === SyncState.Syncing) {
				logger.info('App state sync key arrived, triggering app state sync')
				await doAppStateSync()
			}
		}

		if (type === 'notify') {
			// Run heavy processing in the background to reduce live message latency
			void runProcessing().catch(err => {
				logger.warn({ err }, 'background message processing failed')
			})
			return
		}

		await runProcessing()
	})

	ws.on('CB:presence', handlePresenceUpdate)
	ws.on('CB:chatstate', handlePresenceUpdate)

	ws.on('CB:ib,,dirty', async (node: BinaryNode) => {
		const { attrs } = getBinaryNodeChild(node, 'dirty')!
		const type = attrs.type
		switch (type) {
			case 'account_sync':
				if (attrs.timestamp) {
					let { lastAccountSyncTimestamp } = authState.creds
					if (lastAccountSyncTimestamp) {
						await cleanDirtyBits('account_sync', lastAccountSyncTimestamp)
					}

					lastAccountSyncTimestamp = +attrs.timestamp
					ev.emit('creds.update', { lastAccountSyncTimestamp })
				}

				break
			case 'groups':
				// handled in groups.ts
				break
			default:
				logger.info({ node }, 'received unknown sync')
				break
		}
	})

	ev.on('connection.update', ({ connection, receivedPendingNotifications }) => {
		if (connection === 'open') {
			if (fireInitQueries) {
				executeInitQueries().catch(error => onUnexpectedError(error, 'init queries'))
			}

			sendPresenceUpdate(markOnlineOnConnect ? 'available' : 'unavailable').catch(error =>
				onUnexpectedError(error, 'presence update requests')
			)
		}

		if (!receivedPendingNotifications || syncState !== SyncState.Connecting) {
			return
		}

		syncState = SyncState.AwaitingInitialSync
		logger.info('Connection is now AwaitingInitialSync, buffering events')
		ev.buffer()

		const willSyncHistory = shouldSyncHistoryMessage(
			proto.Message.HistorySyncNotification.create({
				syncType: proto.HistorySync.HistorySyncType.RECENT
			})
		)

		if (!willSyncHistory) {
			logger.info('History sync is disabled by config, not waiting for notification. Transitioning to Online.')
			syncState = SyncState.Online
			setTimeout(() => ev.flush(), 0)
			return
		}

		logger.info('History sync is enabled, awaiting notification with a 20s timeout.')

		if (awaitingSyncTimeout) {
			clearTimeout(awaitingSyncTimeout)
		}

		awaitingSyncTimeout = setTimeout(() => {
			if (syncState === SyncState.AwaitingInitialSync) {
				// TODO: investigate
				logger.warn('Timeout in AwaitingInitialSync, forcing state to Online and flushing buffer')
				syncState = SyncState.Online
				ev.flush()
			}
		}, 20_000)
	})

	ev.on('lid-mapping.update', async ({ lid, pn }) => {
		try {
			await signalRepository.lidMapping.storeLIDPNMappings([{ lid, pn }])
		} catch (error) {
			logger.warn({ lid, pn, error }, 'Failed to store LID-PN mapping')
		}
	})

	return {
		...sock,
		createCallLink,
		getBotListV2,
		messageMutex,
		receiptMutex,
		appStatePatchMutex,
		notificationMutex,
		fetchPrivacySettings,
		upsertMessage,
		appPatch,
		sendPresenceUpdate,
		presenceSubscribe,
		profilePictureUrl,
		fetchBlocklist,
		fetchStatus,
		fetchDisappearingDuration,
		updateProfilePicture,
		removeProfilePicture,
		updateProfileStatus,
		updateProfileName,
		updateBlockStatus,
		updateDisableLinkPreviewsPrivacy,
		updateCallPrivacy,
		updateMessagesPrivacy,
		updateLastSeenPrivacy,
		updateOnlinePrivacy,
		updateProfilePicturePrivacy,
		updateStatusPrivacy,
		updateReadReceiptsPrivacy,
		updateGroupsAddPrivacy,
		checkWhatsApp,
		updateDefaultDisappearingMode,
		getBusinessProfile,
		resyncAppState,
		chatModify,
		cleanDirtyBits,
		addOrEditContact,
		removeContact,
		addLabel,
		addChatLabel,
		removeChatLabel,
		addMessageLabel,
		removeMessageLabel,
		star,
		addOrEditQuickReply,
		removeQuickReply
	}
}
