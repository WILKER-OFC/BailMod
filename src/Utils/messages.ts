import { Boom } from '@hapi/boom'
import { randomBytes } from 'crypto'
import { promises as fs } from 'fs'
import { type Transform } from 'stream'
import { proto } from '../../WAProto/index.js'
import {
	CALL_AUDIO_PREFIX,
	CALL_VIDEO_PREFIX,
	MEDIA_KEYS,
	type MediaType,
	URL_REGEX,
	WA_DEFAULT_EPHEMERAL
} from '../Defaults'
import type {
	AnyMediaMessageContent,
	AnyMessageContent,
	DownloadableMessage,
	MessageContentGenerationOptions,
	MessageGenerationOptions,
	MessageGenerationOptionsFromContent,
	MessageUserReceipt,
	MessageWithContextInfo,
	WAMediaUpload,
	WAMessage,
	WAMessageContent,
	WAMessageKey,
	WATextMessage
} from '../Types'
import { WAMessageStatus, WAProto } from '../Types'
import { isJidGroup, isJidNewsletter, isJidStatusBroadcast, jidNormalizedUser } from '../WABinary'
import { sha256 } from './crypto'
import { generateMessageIDV2, getKeyAuthor, unixTimestampSeconds } from './generics'
import type { ILogger } from './logger'
import {
	downloadContentFromMessage,
	encryptedStream,
	generateThumbnail,
	convertAudioToOggOpus,
	getAudioDuration,
	getAudioWaveform,
	getAudioMetadata,
	getVideoDuration,
	getVideoMetadata,
	getRawMediaUploadData,
	type MediaDownloadOptions
} from './messages-media'
import { shouldIncludeReportingToken } from './reporting-utils'

type ExtractByKey<T, K extends PropertyKey> = T extends Record<K, any> ? T : never
type RequireKey<T, K extends keyof T> = T & {
	[P in K]-?: Exclude<T[P], null | undefined>
}

type WithKey<T, K extends PropertyKey> = T extends unknown ? (K extends keyof T ? RequireKey<T, K> : never) : never

type MediaUploadData = {
	media: WAMediaUpload
	caption?: string
	ptt?: boolean
	ptv?: boolean
	seconds?: number
	gifPlayback?: boolean
	fileName?: string
	jpegThumbnail?: string | Uint8Array
	mimetype?: string
	width?: number
	height?: number
	waveform?: Uint8Array
	backgroundArgb?: number
}

const normalizeJpegThumbnail = (thumb: unknown): Uint8Array | undefined => {
	if (!thumb) {
		return undefined
	}

	if (thumb instanceof Uint8Array) {
		return thumb
	}

	if (typeof thumb === 'string') {
		// Accept both plain base64 and data URLs
		const b64 = thumb.startsWith('data:') ? thumb.split(',')[1] : thumb
		if (!b64) {
			return undefined
		}
		try {
			return Buffer.from(b64, 'base64')
		} catch {
			return undefined
		}
	}

	return undefined
}

const inferAudioMimetype = (media: WAMediaUpload, pathHint?: string): string | undefined => {
	const fromExt = (p: string) => {
		const lower = p.toLowerCase()
		if (lower.endsWith('.mp3')) return 'audio/mpeg'
		if (lower.endsWith('.m4a') || lower.endsWith('.mp4')) return 'audio/mp4'
		if (lower.endsWith('.wav')) return 'audio/wav'
		if (lower.endsWith('.ogg') || lower.endsWith('.opus')) return 'audio/ogg; codecs=opus'
		return undefined
	}

	try {
		if (typeof pathHint === 'string') {
			const mt = fromExt(pathHint)
			if (mt) return mt
		}

		if (Buffer.isBuffer(media)) {
			if (media.length >= 4 && media.subarray(0, 4).toString() === 'OggS') return 'audio/ogg; codecs=opus'
			if (media.length >= 3 && media.subarray(0, 3).toString() === 'ID3') return 'audio/mpeg'
			if (media.length >= 2 && media[0] === 0xff && ((media[1]! & 0xe0) === 0xe0)) return 'audio/mpeg'
			if (media.length >= 12 && media.subarray(0, 4).toString() === 'RIFF' && media.subarray(8, 12).toString() === 'WAVE') {
				return 'audio/wav'
			}
			if (media.length >= 12 && media.subarray(4, 8).toString() === 'ftyp') return 'audio/mp4'
			return undefined
		}

		if (typeof media === 'object' && media && 'url' in media) {
			const u = media.url?.toString?.() || ''
			if (u) {
				const mt = fromExt(u)
				if (mt) return mt
			}
		}
	} catch {
		// ignore
	}

	return undefined
}

const MIMETYPE_MAP: { [T in MediaType]?: string } = {
	image: 'image/jpeg',
	video: 'video/mp4',
	document: 'application/pdf',
	audio: 'audio/ogg; codecs=opus',
	sticker: 'image/webp',
	'product-catalog-image': 'image/jpeg'
}

const MessageTypeProto = {
	image: WAProto.Message.ImageMessage,
	video: WAProto.Message.VideoMessage,
	audio: WAProto.Message.AudioMessage,
	sticker: WAProto.Message.StickerMessage,
	document: WAProto.Message.DocumentMessage
} as const

/**
 * Uses a regex to test whether the string contains a URL, and returns the URL if it does.
 * @param text eg. hello https://google.com
 * @returns the URL, eg. https://google.com
 */
export const extractUrlFromText = (text: string) => text.match(URL_REGEX)?.[0]

export const generateLinkPreviewIfRequired = async (
	text: string,
	getUrlInfo: MessageGenerationOptions['getUrlInfo'],
	logger: MessageGenerationOptions['logger']
) => {
	const url = extractUrlFromText(text)
	if (!!getUrlInfo && url) {
		try {
			const urlInfo = await getUrlInfo(url)
			return urlInfo
		} catch (error: any) {
			// ignore if fails
			logger?.warn({ trace: error.stack }, 'url generation failed')
		}
	}
}

const assertColor = async (color: any) => {
	let assertedColor
	if (typeof color === 'number') {
		assertedColor = color > 0 ? color : 0xffffffff + Number(color) + 1
	} else {
		let hex = color.trim().replace('#', '')
		if (hex.length <= 6) {
			hex = 'FF' + hex.padStart(6, '0')
		}

		assertedColor = parseInt(hex, 16)
		return assertedColor
	}
}

export const prepareWAMessageMedia = async (
	message: AnyMediaMessageContent,
	options: MessageContentGenerationOptions
) => {
	const logger = options.logger

	let mediaType: (typeof MEDIA_KEYS)[number] | undefined
	for (const key of MEDIA_KEYS) {
		if (key in message) {
			mediaType = key
		}
	}

	if (!mediaType) {
		throw new Boom('Invalid media type', { statusCode: 400 })
	}

	const uploadData: MediaUploadData = {
		...message,
		media: (message as any)[mediaType]
	}
	delete (uploadData as any)[mediaType]
	// Normalize optional user-provided thumbnail into bytes
	uploadData.jpegThumbnail = normalizeJpegThumbnail((uploadData as any).jpegThumbnail)
	const mimetypeProvidedByUser = typeof uploadData.mimetype === 'string' && uploadData.mimetype.length > 0
	// check if cacheable + generate cache key
	let cacheableKey =
		typeof uploadData.media === 'object' &&
		'url' in uploadData.media &&
		!!uploadData.media.url &&
		!!options.mediaCache &&
		mediaType + ':' + uploadData.media.url.toString()

	if (mediaType === 'document' && !uploadData.fileName) {
		const mt = uploadData.mimetype || ''
		if (mt.startsWith('video/')) {
			// Helps WhatsApp render a playable document-card with thumbnail
			uploadData.fileName = mt.includes('mp4') ? 'video.mp4' : 'video'
		} else if (mt.startsWith('audio/')) {
			uploadData.fileName = mt.includes('mpeg') ? 'audio.mp3' : 'audio'
		} else {
			uploadData.fileName = 'file'
		}
	}

	if (!uploadData.mimetype) {
		uploadData.mimetype = MIMETYPE_MAP[mediaType]
	}

	// --- PTT (voice note) fix ---
	// WhatsApp expects voice notes (ptt:true) to be OGG/Opus.
	// If the user provides mp3/m4a/etc, the client may show it as corrupted.
	let pttConvertedFilePath: string | undefined
	let pttTempInputFilePath: string | undefined
	let audioConvertedFilePath: string | undefined
	let audioTempInputFilePath: string | undefined

	const isLikelyOggContainer = async (media: WAMediaUpload) => {
		try {
			if (Buffer.isBuffer(media)) {
				return media.length >= 4 && media.subarray(0, 4).toString() === 'OggS'
			}

			if (typeof media === 'object' && media && 'url' in media) {
				const u = media.url?.toString?.() || ''
				// Only attempt local file checks
				if (u && !u.startsWith('http://') && !u.startsWith('https://') && !u.startsWith('data:')) {
					const handle = await fs.open(u, 'r')
					try {
						const hdr = Buffer.alloc(4)
						await handle.read(hdr, 0, 4, 0)
						return hdr.toString() === 'OggS'
					} finally {
						await handle.close()
					}
				}
			}
		} catch {
			// ignore
		}

		return false
	}

	const ensurePttOggOpus = async () => {
		if (mediaType !== 'audio' || uploadData.ptt !== true) {
			return
		}

		// If it's already an OGG container, do not re-encode
		if (await isLikelyOggContainer(uploadData.media)) {
			// keep mimetype correct
			uploadData.mimetype = 'audio/ogg; codecs=opus'
			return
		}

		logger?.debug('ptt:true detected, re-encoding audio to OGG/Opus via ffmpeg')

		// Materialize input if needed
		let inputPath: string | undefined
		if (typeof uploadData.media === 'object' && uploadData.media && 'url' in uploadData.media) {
			const u = uploadData.media.url?.toString?.() || ''
			if (u && !u.startsWith('http://') && !u.startsWith('https://') && !u.startsWith('data:')) {
				inputPath = u
			}
		}

		if (!inputPath) {
			const { filePath } = await getRawMediaUploadData(
				uploadData.media,
				options.mediaTypeOverride || mediaType,
				logger
			)
			pttTempInputFilePath = filePath
			inputPath = filePath
		}

		pttConvertedFilePath = `${inputPath}.ptt.ogg`
		try {
			await convertAudioToOggOpus(inputPath, pttConvertedFilePath, logger)
			// Ensure output is really an OGG container before marking as ptt
			try {
				const handle = await fs.open(pttConvertedFilePath, 'r')
				try {
					const hdr = Buffer.alloc(4)
					await handle.read(hdr, 0, 4, 0)
					if (hdr.toString() !== 'OggS') {
						throw new Error('PTT conversion output is not OGG')
					}
				} finally {
					await handle.close()
				}
			} catch (e) {
				logger?.warn({ err: e }, 'PTT conversion produced invalid container; falling back to normal audio')
				throw e
			}

			// Validate codec/format with ffprobe if available
			const meta = await getAudioMetadata(pttConvertedFilePath, logger)
			if (meta) {
				const format = meta.format || ''
				const codec = meta.codec || ''
				const isOgg = format.split(',').some(f => f.trim() === 'ogg')
				if (!isOgg || codec !== 'opus') {
					throw new Error(`PTT conversion invalid format/codec: ${format}/${codec}`)
				}
			}
		} catch (err) {
			logger?.warn({ err }, 'ffmpeg PTT re-encode failed, falling back to original audio')
			// Clean up any temp input we created (never delete user's file)
			if (pttTempInputFilePath) {
				await fs.unlink(pttTempInputFilePath).catch(() => {})
				pttTempInputFilePath = undefined
			}
			// Remove failed output file if it was created
			if (pttConvertedFilePath) {
				await fs.unlink(pttConvertedFilePath).catch(() => {})
				pttConvertedFilePath = undefined
			}
			// If we can't produce a valid voice-note (OGG/Opus), send as normal audio instead
			uploadData.ptt = false
			if (!mimetypeProvidedByUser) {
				uploadData.mimetype = inferAudioMimetype(uploadData.media, inputPath) || 'audio/mpeg'
			}
			return
		}
		// Do not use mediaCache for re-encoded PTT
		cacheableKey = false

		// If we created a temp input file, remove it (never delete user's local file)
		if (pttTempInputFilePath) {
			await fs.unlink(pttTempInputFilePath).catch(() => {})
			pttTempInputFilePath = undefined
		}

		uploadData.media = { url: pttConvertedFilePath }
		uploadData.mimetype = 'audio/ogg; codecs=opus'
	}

	const ensureAudioOpusForMp4 = async () => {
		if (mediaType !== 'audio' || uploadData.ptt === true) {
			return
		}

		const mt = uploadData.mimetype || ''
		if (!mt.startsWith('audio/mp4') && !mt.startsWith('audio/aac')) {
			return
		}

		// If it's already an OGG container, do not re-encode
		if (await isLikelyOggContainer(uploadData.media)) {
			uploadData.mimetype = 'audio/ogg; codecs=opus'
			return
		}

		logger?.debug('audio/mp4 detected, converting to OGG/Opus via ffmpeg')

		// Materialize input if needed
		let inputPath: string | undefined
		if (typeof uploadData.media === 'object' && uploadData.media && 'url' in uploadData.media) {
			const u = uploadData.media.url?.toString?.() || ''
			if (u && !u.startsWith('http://') && !u.startsWith('https://') && !u.startsWith('data:')) {
				inputPath = u
			}
		}

		if (!inputPath) {
			const { filePath } = await getRawMediaUploadData(
				uploadData.media,
				options.mediaTypeOverride || mediaType,
				logger
			)
			audioTempInputFilePath = filePath
			inputPath = filePath
		}

		audioConvertedFilePath = `${inputPath}.opus.ogg`
		try {
			await convertAudioToOggOpus(inputPath, audioConvertedFilePath, logger)
			const meta = await getAudioMetadata(audioConvertedFilePath, logger)
			if (meta) {
				const format = meta.format || ''
				const codec = meta.codec || ''
				const isOgg = format.split(',').some(f => f.trim() === 'ogg')
				if (!isOgg || codec !== 'opus') {
					throw new Error(`audio conversion invalid format/codec: ${format}/${codec}`)
				}
			}
		} catch (err) {
			logger?.warn({ err }, 'audio/mp4 re-encode failed, sending original')
			if (audioTempInputFilePath) {
				await fs.unlink(audioTempInputFilePath).catch(() => {})
				audioTempInputFilePath = undefined
			}
			if (audioConvertedFilePath) {
				await fs.unlink(audioConvertedFilePath).catch(() => {})
				audioConvertedFilePath = undefined
			}
			return
		}

		cacheableKey = false
		if (audioTempInputFilePath) {
			await fs.unlink(audioTempInputFilePath).catch(() => {})
			audioTempInputFilePath = undefined
		}

		uploadData.media = { url: audioConvertedFilePath }
		uploadData.mimetype = 'audio/ogg; codecs=opus'
	}

	await ensurePttOggOpus()
	await ensureAudioOpusForMp4()

	if (cacheableKey) {
		const mediaBuff = await options.mediaCache!.get<Buffer>(cacheableKey)
		if (mediaBuff) {
			logger?.debug({ cacheableKey }, 'got media cache hit')

			const obj = proto.Message.decode(mediaBuff)
			const key = `${mediaType}Message`

			Object.assign(obj[key as keyof proto.Message]!, { ...uploadData, media: undefined })

			return obj
		}
	}

	const isNewsletter = !!options.jid && isJidNewsletter(options.jid)
	if (isNewsletter) {
		logger?.info({ key: cacheableKey }, 'Preparing raw media for newsletter')
		const { filePath, fileSha256, fileLength } = await getRawMediaUploadData(
			uploadData.media,
			options.mediaTypeOverride || mediaType,
			logger
		)

		const fileSha256B64 = fileSha256.toString('base64')

		const isVideoDocument =
			mediaType === 'document' && typeof uploadData.mimetype === 'string' && uploadData.mimetype.startsWith('video/')
		const requiresThumb =
			(mediaType === 'image' || mediaType === 'video' || isVideoDocument) && typeof uploadData['jpegThumbnail'] === 'undefined'
		const requiresVideoMeta = mediaType === 'video' && (typeof uploadData.seconds === 'undefined' || !uploadData.width || !uploadData.height)
		const thumbType = (isVideoDocument ? 'video' : mediaType) as 'image' | 'video'
		const thumbWidth = thumbType === 'video' ? 256 : 32

		const [{ mediaUrl, directPath }, thumbRes, videoMeta] = await Promise.all([
			options.upload(filePath, {
				fileEncSha256B64: fileSha256B64,
				mediaType: mediaType,
				timeoutMs: options.mediaUploadTimeoutMs
			}),
			requiresThumb ? generateThumbnail(filePath, thumbType, options, thumbWidth) : Promise.resolve(undefined),
			requiresVideoMeta ? getVideoMetadata(filePath, logger) : Promise.resolve(undefined)
		])

		if (thumbRes?.thumbnail) {
			uploadData.jpegThumbnail = thumbRes.thumbnail
			if (!uploadData.width && thumbRes.originalImageDimensions) {
				uploadData.width = thumbRes.originalImageDimensions.width
				uploadData.height = thumbRes.originalImageDimensions.height
			}
		}
		if (videoMeta) {
			if (typeof uploadData.seconds === 'undefined' && typeof videoMeta.duration === 'number') {
				uploadData.seconds = Math.max(1, Math.ceil(videoMeta.duration))
			}
			if (!uploadData.width && typeof videoMeta.width === 'number') uploadData.width = videoMeta.width
			if (!uploadData.height && typeof videoMeta.height === 'number') uploadData.height = videoMeta.height
		}

		await fs.unlink(filePath)
		if (pttConvertedFilePath) {
			await fs.unlink(pttConvertedFilePath).catch(() => {})
			pttConvertedFilePath = undefined
		}

		const obj = WAProto.Message.fromObject({
			// todo: add more support here
			[`${mediaType}Message`]: (MessageTypeProto as any)[mediaType].fromObject({
				url: mediaUrl,
				directPath,
				fileSha256,
				fileLength,
				...uploadData,
				media: undefined
			})
		})

		if (uploadData.ptv) {
			obj.ptvMessage = obj.videoMessage
			delete obj.videoMessage
		}

		if (obj.stickerMessage) {
			obj.stickerMessage.stickerSentTs = Date.now()
		}

		if (cacheableKey) {
			logger?.debug({ cacheableKey }, 'set cache')
			await options.mediaCache!.set(cacheableKey, WAProto.Message.encode(obj).finish())
		}

		return obj
	}

	const requiresAudioDurationComputation = mediaType === 'audio' && typeof uploadData.seconds === 'undefined'
	const requiresVideoMetadataComputation =
		mediaType === 'video' &&
		(typeof uploadData.seconds === 'undefined' || typeof uploadData.width === 'undefined' || typeof uploadData.height === 'undefined')
	const requiresDurationComputation = requiresAudioDurationComputation || requiresVideoMetadataComputation
	const isVideoDocument =
		mediaType === 'document' && typeof uploadData.mimetype === 'string' && uploadData.mimetype.startsWith('video/')
	const requiresThumbnailComputation =
		(mediaType === 'image' || mediaType === 'video' || isVideoDocument) &&
		typeof uploadData['jpegThumbnail'] === 'undefined'
	const requiresWaveformProcessing = mediaType === 'audio' && uploadData.ptt === true
	const requiresAudioBackground = options.backgroundColor && mediaType === 'audio' && uploadData.ptt === true
	const requiresOriginalForSomeProcessing =
		requiresDurationComputation || requiresThumbnailComputation || requiresWaveformProcessing
	const { mediaKey, encFilePath, originalFilePath, fileEncSha256, fileSha256, fileLength } = await encryptedStream(
		uploadData.media,
		options.mediaTypeOverride || mediaType,
		{
			logger,
			saveOriginalFileIfRequired: requiresOriginalForSomeProcessing,
			opts: options.options
		}
	)

	const fileEncSha256B64 = fileEncSha256.toString('base64')
	const [{ mediaUrl, directPath }] = await Promise.all([
		(async () => {
			const result = await options.upload(encFilePath, {
				fileEncSha256B64,
				mediaType,
				timeoutMs: options.mediaUploadTimeoutMs
			})
			logger?.debug({ mediaType, cacheableKey }, 'uploaded media')
			return result
		})(),
		(async () => {
			try {
				const thumbType = (isVideoDocument ? 'video' : mediaType) as 'image' | 'video'
				const thumbWidth = thumbType === 'video' ? 256 : 32

				const [thumbRes, audioDur, videoMeta, waveform, bg] = await Promise.all([
					requiresThumbnailComputation
						? generateThumbnail(originalFilePath!, thumbType, options, thumbWidth)
						: Promise.resolve(undefined),
					requiresAudioDurationComputation ? getAudioDuration(originalFilePath!) : Promise.resolve(undefined),
					requiresVideoMetadataComputation ? getVideoMetadata(originalFilePath!, logger) : Promise.resolve(undefined),
					requiresWaveformProcessing ? getAudioWaveform(originalFilePath!, logger) : Promise.resolve(undefined),
					requiresAudioBackground ? assertColor(options.backgroundColor) : Promise.resolve(undefined)
				])

				if (thumbRes?.thumbnail) {
					uploadData.jpegThumbnail = thumbRes.thumbnail
					if (!uploadData.width && thumbRes.originalImageDimensions) {
						uploadData.width = thumbRes.originalImageDimensions.width
						uploadData.height = thumbRes.originalImageDimensions.height
						logger?.debug('set dimensions')
					}
					logger?.debug('generated thumbnail')
				}

				if (requiresAudioDurationComputation) {
					const dur = audioDur
					if (typeof dur === 'number' && Number.isFinite(dur)) {
						uploadData.seconds = Math.max(1, Math.ceil(dur))
						logger?.debug('computed audio duration')
					}
				}

				if (requiresVideoMetadataComputation && videoMeta) {
					if (typeof uploadData.seconds === 'undefined' && typeof videoMeta.duration === 'number') {
						uploadData.seconds = Math.max(1, Math.ceil(videoMeta.duration))
						logger?.debug('computed video duration')
					}
					if (typeof uploadData.width === 'undefined' && typeof videoMeta.width === 'number') {
						uploadData.width = videoMeta.width
					}
					if (typeof uploadData.height === 'undefined' && typeof videoMeta.height === 'number') {
						uploadData.height = videoMeta.height
					}
				}

				if (requiresWaveformProcessing && waveform) {
					uploadData.waveform = waveform
					logger?.debug('processed waveform')
				}

				if (requiresAudioBackground && typeof bg === 'number') {
					uploadData.backgroundArgb = bg
					logger?.debug('computed backgroundColor audio status')
				}
			} catch (error) {
				logger?.warn({ trace: (error as any).stack }, 'failed to obtain extra info')
			}
		})()
	]).finally(async () => {
		try {
			await fs.unlink(encFilePath)
			if (originalFilePath) {
				await fs.unlink(originalFilePath)
			}
			if (pttConvertedFilePath) {
				await fs.unlink(pttConvertedFilePath).catch(() => {})
				pttConvertedFilePath = undefined
			}
			if (audioConvertedFilePath) {
				await fs.unlink(audioConvertedFilePath).catch(() => {})
				audioConvertedFilePath = undefined
			}

			logger?.debug('removed tmp files')
		} catch (error) {
			logger?.warn('failed to remove tmp file')
		}
	})

	const obj = WAProto.Message.fromObject({
		[`${mediaType}Message`]: MessageTypeProto[mediaType as keyof typeof MessageTypeProto].fromObject({
			url: mediaUrl,
			directPath,
			mediaKey,
			fileEncSha256,
			fileSha256,
			fileLength,
			mediaKeyTimestamp: unixTimestampSeconds(),
			...uploadData,
			media: undefined
		} as any)
	})

	if (uploadData.ptv) {
		obj.ptvMessage = obj.videoMessage
		delete obj.videoMessage
	}

	if (cacheableKey) {
		logger?.debug({ cacheableKey }, 'set cache')
		await options.mediaCache!.set(cacheableKey, WAProto.Message.encode(obj).finish())
	}

	return obj
}

export const prepareDisappearingMessageSettingContent = (ephemeralExpiration?: number) => {
	ephemeralExpiration = ephemeralExpiration || 0
	const content: WAMessageContent = {
		ephemeralMessage: {
			message: {
				protocolMessage: {
					type: WAProto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
					ephemeralExpiration
				}
			}
		}
	}
	return WAProto.Message.fromObject(content)
}

/**
 * Generate forwarded message content like WA does
 * @param message the message to forward
 * @param options.forceForward will show the message as forwarded even if it is from you
 */
export const generateForwardMessageContent = (message: WAMessage, forceForward?: boolean) => {
	let content = message.message
	if (!content) {
		throw new Boom('no content in message', { statusCode: 400 })
	}

	// hacky copy
	content = normalizeMessageContent(content)
	content = proto.Message.decode(proto.Message.encode(content!).finish())

	let key = Object.keys(content)[0] as keyof proto.IMessage

	let score = (content?.[key] as { contextInfo: proto.IContextInfo })?.contextInfo?.forwardingScore || 0
	score += message.key.fromMe && !forceForward ? 0 : 1
	if (key === 'conversation') {
		content.extendedTextMessage = { text: content[key] }
		delete content.conversation

		key = 'extendedTextMessage'
	}

	const key_ = content?.[key] as { contextInfo: proto.IContextInfo }
	if (score > 0) {
		key_.contextInfo = { forwardingScore: score, isForwarded: true }
	} else {
		key_.contextInfo = {}
	}

	return content
}

export const hasNonNullishProperty = <K extends PropertyKey>(
	message: AnyMessageContent,
	key: K
): message is ExtractByKey<AnyMessageContent, K> => {
	return (
		typeof message === 'object' &&
		message !== null &&
		key in message &&
		(message as any)[key] !== null &&
		(message as any)[key] !== undefined
	)
}

function hasOptionalProperty<T, K extends PropertyKey>(obj: T, key: K): obj is WithKey<T, K> {
	return typeof obj === 'object' && obj !== null && key in obj && (obj as any)[key] !== null
}

export const generateWAMessageContent = async (
	message: AnyMessageContent,
	options: MessageContentGenerationOptions
) => {
	let m: WAMessageContent = {}
	if (hasNonNullishProperty(message, 'text')) {
		const extContent = { text: message.text } as WATextMessage

		let urlInfo = message.linkPreview
		if (typeof urlInfo === 'undefined') {
			urlInfo = await generateLinkPreviewIfRequired(message.text, options.getUrlInfo, options.logger)
		}

		if (urlInfo) {
			extContent.matchedText = urlInfo['matched-text']
			extContent.jpegThumbnail = urlInfo.jpegThumbnail
			extContent.description = urlInfo.description
			extContent.title = urlInfo.title
			extContent.previewType = 0

			const img = urlInfo.highQualityThumbnail
			if (img) {
				extContent.thumbnailDirectPath = img.directPath
				extContent.mediaKey = img.mediaKey
				extContent.mediaKeyTimestamp = img.mediaKeyTimestamp
				extContent.thumbnailWidth = img.width
				extContent.thumbnailHeight = img.height
				extContent.thumbnailSha256 = img.fileSha256
				extContent.thumbnailEncSha256 = img.fileEncSha256
			}
		}

		if (options.backgroundColor) {
			extContent.backgroundArgb = await assertColor(options.backgroundColor)
		}

		if (options.font) {
			extContent.font = options.font
		}

		m.extendedTextMessage = extContent
	} else if (hasNonNullishProperty(message, 'contacts')) {
		const contactLen = message.contacts.contacts.length
		if (!contactLen) {
			throw new Boom('require atleast 1 contact', { statusCode: 400 })
		}

		if (contactLen === 1) {
			m.contactMessage = WAProto.Message.ContactMessage.create(message.contacts.contacts[0])
		} else {
			m.contactsArrayMessage = WAProto.Message.ContactsArrayMessage.create(message.contacts)
		}
	} else if (hasNonNullishProperty(message, 'location')) {
		m.locationMessage = WAProto.Message.LocationMessage.create(message.location)
	} else if (hasNonNullishProperty(message, 'react')) {
		if (!message.react.senderTimestampMs) {
			message.react.senderTimestampMs = Date.now()
		}

		m.reactionMessage = WAProto.Message.ReactionMessage.create(message.react)
	} else if (hasNonNullishProperty(message, 'delete')) {
		m.protocolMessage = {
			key: message.delete,
			type: WAProto.Message.ProtocolMessage.Type.REVOKE
		}
	} else if (hasNonNullishProperty(message, 'forward')) {
		m = generateForwardMessageContent(message.forward, message.force)
	} else if (hasNonNullishProperty(message, 'disappearingMessagesInChat')) {
		const exp =
			typeof message.disappearingMessagesInChat === 'boolean'
				? message.disappearingMessagesInChat
					? WA_DEFAULT_EPHEMERAL
					: 0
				: message.disappearingMessagesInChat
		m = prepareDisappearingMessageSettingContent(exp)
	} else if (hasNonNullishProperty(message, 'groupInvite')) {
		m.groupInviteMessage = {}
		m.groupInviteMessage.inviteCode = message.groupInvite.inviteCode
		m.groupInviteMessage.inviteExpiration = message.groupInvite.inviteExpiration
		m.groupInviteMessage.caption = message.groupInvite.text

		m.groupInviteMessage.groupJid = message.groupInvite.jid
		m.groupInviteMessage.groupName = message.groupInvite.subject
		//TODO: use built-in interface and get disappearing mode info etc.
		//TODO: cache / use store!?
		if (options.getProfilePicUrl) {
			const pfpUrl = await options.getProfilePicUrl(message.groupInvite.jid, 'preview')
			if (pfpUrl) {
				const resp = await fetch(pfpUrl, { method: 'GET', dispatcher: options?.options?.dispatcher })
				if (resp.ok) {
					const buf = Buffer.from(await resp.arrayBuffer())
					m.groupInviteMessage.jpegThumbnail = buf
				}
			}
		}
	} else if (hasNonNullishProperty(message, 'pin')) {
		m.pinInChatMessage = {}
		m.messageContextInfo = {}

		m.pinInChatMessage.key = message.pin
		m.pinInChatMessage.type = message.type
		m.pinInChatMessage.senderTimestampMs = Date.now()

		m.messageContextInfo.messageAddOnDurationInSecs = message.type === 1 ? message.time || 86400 : 0
	} else if (hasNonNullishProperty(message, 'buttonReply')) {
		switch (message.type) {
			case 'template':
				m.templateButtonReplyMessage = {
					selectedDisplayText: message.buttonReply.displayText,
					selectedId: message.buttonReply.id,
					selectedIndex: message.buttonReply.index
				}
				break
			case 'plain':
				m.buttonsResponseMessage = {
					selectedButtonId: message.buttonReply.id,
					selectedDisplayText: message.buttonReply.displayText,
					type: proto.Message.ButtonsResponseMessage.Type.DISPLAY_TEXT
				}
				break
		}
	} else if (hasOptionalProperty(message, 'ptv') && message.ptv) {
		const { videoMessage } = await prepareWAMessageMedia({ video: message.video }, options)
		m.ptvMessage = videoMessage
	} else if (hasNonNullishProperty(message, 'product')) {
		const { imageMessage } = await prepareWAMessageMedia({ image: message.product.productImage }, options)
		m.productMessage = WAProto.Message.ProductMessage.create({
			...message,
			product: {
				...message.product,
				productImage: imageMessage
			}
		})
	} else if (hasNonNullishProperty(message, 'listReply')) {
		m.listResponseMessage = { ...message.listReply }
	} else if (hasNonNullishProperty(message, 'event')) {
		m.eventMessage = {}
		const startTime = Math.floor(message.event.startDate.getTime() / 1000)

		if (message.event.call && options.getCallLink) {
			const token = await options.getCallLink(message.event.call, { startTime })
			m.eventMessage.joinLink = (message.event.call === 'audio' ? CALL_AUDIO_PREFIX : CALL_VIDEO_PREFIX) + token
		}

		m.messageContextInfo = {
			// encKey
			messageSecret: message.event.messageSecret || randomBytes(32)
		}

		m.eventMessage.name = message.event.name
		m.eventMessage.description = message.event.description
		m.eventMessage.startTime = startTime
		m.eventMessage.endTime = message.event.endDate ? message.event.endDate.getTime() / 1000 : undefined
		m.eventMessage.isCanceled = message.event.isCancelled ?? false
		m.eventMessage.extraGuestsAllowed = message.event.extraGuestsAllowed
		m.eventMessage.isScheduleCall = message.event.isScheduleCall ?? false
		m.eventMessage.location = message.event.location
	} else if (hasNonNullishProperty(message, 'poll')) {
		message.poll.selectableCount ||= 0
		message.poll.toAnnouncementGroup ||= false

		if (!Array.isArray(message.poll.values)) {
			throw new Boom('Invalid poll values', { statusCode: 400 })
		}

		if (message.poll.selectableCount < 0 || message.poll.selectableCount > message.poll.values.length) {
			throw new Boom(`poll.selectableCount in poll should be >= 0 and <= ${message.poll.values.length}`, {
				statusCode: 400
			})
		}

		m.messageContextInfo = {
			// encKey
			messageSecret: message.poll.messageSecret || randomBytes(32)
		}

		const pollCreationMessage = {
			name: message.poll.name,
			selectableOptionsCount: message.poll.selectableCount,
			options: message.poll.values.map(optionName => ({ optionName }))
		}

		if (message.poll.toAnnouncementGroup) {
			// poll v2 is for community announcement groups (single select and multiple)
			m.pollCreationMessageV2 = pollCreationMessage
		} else {
			if (message.poll.selectableCount === 1) {
				//poll v3 is for single select polls
				m.pollCreationMessageV3 = pollCreationMessage
			} else {
				// poll for multiple choice polls
				m.pollCreationMessage = pollCreationMessage
			}
		}
	} else if (hasNonNullishProperty(message, 'sharePhoneNumber')) {
		m.protocolMessage = {
			type: proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER
		}
	} else if (hasNonNullishProperty(message, 'requestPhoneNumber')) {
		m.requestPhoneNumberMessage = {}
	} else if (hasNonNullishProperty(message, 'limitSharing')) {
		m.protocolMessage = {
			type: proto.Message.ProtocolMessage.Type.LIMIT_SHARING,
			limitSharing: {
				sharingLimited: message.limitSharing === true,
				trigger: 1,
				limitSharingSettingTimestamp: Date.now(),
				initiatedByMe: true
			}
		}
	} else {
		m = await prepareWAMessageMedia(message, options)
	}

	// ---- BUTTONS & INTERACTIVE CONTENT (ported from upstream baileys) ----
	const ButtonType = proto.Message.ButtonsMessage.HeaderType
	if ('buttons' in message && !!message.buttons) {
		const buttonsMessage: proto.Message.IButtonsMessage = {
			buttons: message.buttons.map(b => ({ ...b, type: proto.Message.ButtonsMessage.Button.Type.RESPONSE }))
		}

		if ('text' in message) {
			buttonsMessage.contentText = message.text
			buttonsMessage.headerType = ButtonType.EMPTY
		} else {
			if ('caption' in message) {
				buttonsMessage.contentText = message.caption
			}

			const headerKey = Object.keys(m)[0]
			const type = headerKey?.replace('Message', '').toUpperCase()
			buttonsMessage.headerType = type ? ((ButtonType as any)[type] ?? ButtonType.EMPTY) : ButtonType.EMPTY
			Object.assign(buttonsMessage, m)
		}

		if ('title' in message && !!message.title) {
			buttonsMessage.text = message.title
			buttonsMessage.headerType = ButtonType.TEXT
		}
		if ('footer' in message && !!message.footer) {
			buttonsMessage.footerText = message.footer
		}
		if ('contextInfo' in message && !!message.contextInfo) {
			buttonsMessage.contextInfo = message.contextInfo
		}
		if ('mentions' in message && !!message.mentions) {
			buttonsMessage.contextInfo = { mentionedJid: message.mentions }
		}

		m = { buttonsMessage }
	} else if ('templateButtons' in message && !!message.templateButtons) {
		const msg: proto.Message.TemplateMessage.IHydratedFourRowTemplate = {
			hydratedButtons: message.templateButtons
		}

		if ('text' in message) {
			msg.hydratedContentText = message.text
		} else {
			if ('caption' in message) {
				msg.hydratedContentText = message.caption
			}
			Object.assign(msg, m)
		}

		if ('footer' in message && !!message.footer) {
			msg.hydratedFooterText = message.footer
		}

		m = {
			templateMessage: {
				fourRowTemplate: msg,
				hydratedTemplate: msg
			}
		}
	}

	if ('sections' in message && !!message.sections) {
		const listMessage: proto.Message.IListMessage = {
			sections: message.sections,
			buttonText: message.buttonText,
			title: message.title,
			footerText: 'footer' in message ? message.footer : undefined,
			description: 'text' in message ? message.text : undefined,
			listType: message.listType ?? proto.Message.ListMessage.ListType.SINGLE_SELECT
		}
		m = { listMessage }
	}

	if ('interactiveButtons' in message && !!message.interactiveButtons) {
		const interactiveMessage: proto.Message.IInteractiveMessage = {
			nativeFlowMessage: WAProto.Message.InteractiveMessage.NativeFlowMessage.fromObject({
				buttons: message.interactiveButtons
			})
		}

		if ('text' in message) {
			interactiveMessage.body = { text: message.text }
		} else if ('caption' in message) {
			interactiveMessage.body = { text: message.caption }
			interactiveMessage.header = {
				title: message.title,
				subtitle: message.subtitle,
				hasMediaAttachment: message.media ?? false
			}
			Object.assign(interactiveMessage.header, m)
		}

		if ('footer' in message && !!message.footer) {
			interactiveMessage.footer = { text: message.footer }
		}

		if ('title' in message && !!message.title) {
			interactiveMessage.header = {
				title: message.title,
				subtitle: message.subtitle,
				hasMediaAttachment: message.media ?? false
			}
			Object.assign(interactiveMessage.header, m)
		}

		if ('contextInfo' in message && !!message.contextInfo) {
			interactiveMessage.contextInfo = message.contextInfo
		}
		if ('mentions' in message && !!message.mentions) {
			interactiveMessage.contextInfo = { mentionedJid: message.mentions }
		}

		m = { interactiveMessage }
	}

	if ('shop' in message && !!message.shop) {
		const interactiveMessage: proto.Message.IInteractiveMessage = {
			shopStorefrontMessage: WAProto.Message.InteractiveMessage.ShopMessage.fromObject({
				surface: message.shop,
				id: message.id
			})
		}

		if ('text' in message) {
			interactiveMessage.body = { text: message.text }
		} else if ('caption' in message) {
			interactiveMessage.body = { text: message.caption }
			interactiveMessage.header = {
				title: message.title,
				subtitle: message.subtitle,
				hasMediaAttachment: message.media ?? false
			}
			Object.assign(interactiveMessage.header, m)
		}

		if ('footer' in message && !!message.footer) {
			interactiveMessage.footer = { text: message.footer }
		}

		if ('title' in message && !!message.title) {
			interactiveMessage.header = {
				title: message.title,
				subtitle: message.subtitle,
				hasMediaAttachment: message.media ?? false
			}
			Object.assign(interactiveMessage.header, m)
		}

		if ('contextInfo' in message && !!message.contextInfo) {
			interactiveMessage.contextInfo = message.contextInfo
		}
		if ('mentions' in message && !!message.mentions) {
			interactiveMessage.contextInfo = { mentionedJid: message.mentions }
		}

		m = { interactiveMessage }
	}

	if (hasOptionalProperty(message, 'viewOnce') && !!message.viewOnce) {
		m = { viewOnceMessage: { message: m } }
	}

	if (hasOptionalProperty(message, 'mentions') && message.mentions?.length) {
		const messageType = Object.keys(m)[0]! as Extract<keyof proto.IMessage, MessageWithContextInfo>
		const key = m[messageType]
		if ('contextInfo' in key! && !!key.contextInfo) {
			key.contextInfo.mentionedJid = message.mentions
		} else if (key!) {
			key.contextInfo = {
				mentionedJid: message.mentions
			}
		}
	}

	if (hasOptionalProperty(message, 'edit')) {
		m = {
			protocolMessage: {
				key: message.edit,
				editedMessage: m,
				timestampMs: Date.now(),
				type: WAProto.Message.ProtocolMessage.Type.MESSAGE_EDIT
			}
		}
	}

	if (hasOptionalProperty(message, 'contextInfo') && !!message.contextInfo) {
		const messageType = Object.keys(m)[0]! as Extract<keyof proto.IMessage, MessageWithContextInfo>
		const key = m[messageType]
		if ('contextInfo' in key! && !!key.contextInfo) {
			key.contextInfo = { ...key.contextInfo, ...message.contextInfo }
		} else if (key!) {
			key.contextInfo = message.contextInfo
		}
	}

	if (shouldIncludeReportingToken(m)) {
		m.messageContextInfo = m.messageContextInfo || {}
		if (!m.messageContextInfo.messageSecret) {
			m.messageContextInfo.messageSecret = randomBytes(32)
		}
	}

	return WAProto.Message.create(m)
}

export const generateWAMessageFromContent = (
	jid: string,
	message: WAMessageContent,
	options: MessageGenerationOptionsFromContent
) => {
	// set timestamp to now
	// if not specified
	if (!options.timestamp) {
		options.timestamp = new Date()
	}

	const innerMessage = normalizeMessageContent(message)!
	const key = getContentType(innerMessage)! as Exclude<keyof proto.IMessage, 'conversation'>
	const timestamp = unixTimestampSeconds(options.timestamp)
	const { quoted, userJid } = options

	if (quoted && !isJidNewsletter(jid)) {
		const participant = quoted.key.fromMe
			? userJid // TODO: Add support for LIDs
			: quoted.participant || quoted.key.participant || quoted.key.remoteJid

		let quotedMsg = normalizeMessageContent(quoted.message)!
		const msgType = getContentType(quotedMsg)!
		// strip any redundant properties
		quotedMsg = proto.Message.create({ [msgType]: quotedMsg[msgType] })

		const quotedContent = quotedMsg[msgType]
		if (typeof quotedContent === 'object' && quotedContent && 'contextInfo' in quotedContent) {
			delete quotedContent.contextInfo
		}

		const contextInfo: proto.IContextInfo =
			('contextInfo' in innerMessage[key]! && innerMessage[key]?.contextInfo) || {}
		contextInfo.participant = jidNormalizedUser(participant!)
		contextInfo.stanzaId = quoted.key.id
		contextInfo.quotedMessage = quotedMsg

		// if a participant is quoted, then it must be a group
		// hence, remoteJid of group must also be entered
		if (jid !== quoted.key.remoteJid) {
			contextInfo.remoteJid = quoted.key.remoteJid
		}

		if (contextInfo && innerMessage[key]) {
			/* @ts-ignore */
			innerMessage[key].contextInfo = contextInfo
		}
	}

	if (
		// if we want to send a disappearing message
		!!options?.ephemeralExpiration &&
		// and it's not a protocol message -- delete, toggle disappear message
		key !== 'protocolMessage' &&
		// already not converted to disappearing message
		key !== 'ephemeralMessage' &&
		// newsletters don't support ephemeral messages
		!isJidNewsletter(jid)
	) {
		/* @ts-ignore */
		innerMessage[key].contextInfo = {
			...((innerMessage[key] as any).contextInfo || {}),
			expiration: options.ephemeralExpiration || WA_DEFAULT_EPHEMERAL
			//ephemeralSettingTimestamp: options.ephemeralOptions.eph_setting_ts?.toString()
		}
	}

	message = WAProto.Message.create(message)

	const messageJSON = {
		key: {
			remoteJid: jid,
			fromMe: true,
			id: options?.messageId || generateMessageIDV2()
		},
		message: message,
		messageTimestamp: timestamp,
		messageStubParameters: [],
		participant: isJidGroup(jid) || isJidStatusBroadcast(jid) ? userJid : undefined, // TODO: Add support for LIDs
		status: WAMessageStatus.PENDING
	}
	return WAProto.WebMessageInfo.fromObject(messageJSON) as WAMessage
}

export const generateWAMessage = async (jid: string, content: AnyMessageContent, options: MessageGenerationOptions) => {
	// ensure msg ID is with every log
	options.logger = options?.logger?.child({ msgId: options.messageId })
	// Pass jid in the options to generateWAMessageContent
	return generateWAMessageFromContent(jid, await generateWAMessageContent(content, { ...options, jid }), options)
}

/** Get the key to access the true type of content */
export const getContentType = (content: proto.IMessage | undefined) => {
	if (content) {
		const keys = Object.keys(content)
		const key = keys.find(k => (k === 'conversation' || k.includes('Message')) && k !== 'senderKeyDistributionMessage')
		return key as keyof typeof content
	}
}

/**
 * Normalizes ephemeral, view once messages to regular message content
 * Eg. image messages in ephemeral messages, in view once messages etc.
 * @param content
 * @returns
 */
export const normalizeMessageContent = (content: WAMessageContent | null | undefined): WAMessageContent | undefined => {
	if (!content) {
		return undefined
	}

	// set max iterations to prevent an infinite loop
	for (let i = 0; i < 5; i++) {
		const inner = getFutureProofMessage(content)
		if (!inner) {
			break
		}

		content = inner.message
	}

	return content!

	function getFutureProofMessage(message: typeof content) {
		return (
			message?.ephemeralMessage ||
			message?.viewOnceMessage ||
			message?.documentWithCaptionMessage ||
			message?.viewOnceMessageV2 ||
			message?.viewOnceMessageV2Extension ||
			message?.editedMessage ||
			message?.associatedChildMessage ||
			message?.groupStatusMessage ||
			message?.groupStatusMessageV2
		)
	}
}

/**
 * Extract the true message content from a message
 * Eg. extracts the inner message from a disappearing message/view once message
 */
export const extractMessageContent = (content: WAMessageContent | undefined | null): WAMessageContent | undefined => {
	const extractFromTemplateMessage = (
		msg: proto.Message.TemplateMessage.IHydratedFourRowTemplate | proto.Message.IButtonsMessage
	) => {
		if (msg.imageMessage) {
			return { imageMessage: msg.imageMessage }
		} else if (msg.documentMessage) {
			return { documentMessage: msg.documentMessage }
		} else if (msg.videoMessage) {
			return { videoMessage: msg.videoMessage }
		} else if (msg.locationMessage) {
			return { locationMessage: msg.locationMessage }
		} else {
			return {
				conversation:
					'contentText' in msg ? msg.contentText : 'hydratedContentText' in msg ? msg.hydratedContentText : ''
			}
		}
	}

	content = normalizeMessageContent(content)

	if (content?.buttonsMessage) {
		return extractFromTemplateMessage(content.buttonsMessage)
	}

	if (content?.templateMessage?.hydratedFourRowTemplate) {
		return extractFromTemplateMessage(content?.templateMessage?.hydratedFourRowTemplate)
	}

	if (content?.templateMessage?.hydratedTemplate) {
		return extractFromTemplateMessage(content?.templateMessage?.hydratedTemplate)
	}

	if (content?.templateMessage?.fourRowTemplate) {
		return extractFromTemplateMessage(content?.templateMessage?.fourRowTemplate)
	}

	return content
}

/**
 * Returns the device predicted by message ID
 */
export const getDevice = (id: string) =>
	/^3A.{18}$/.test(id)
		? 'ios'
		: /^3E.{20}$/.test(id)
			? 'web'
			: /^(.{21}|.{32})$/.test(id)
				? 'android'
				: /^(3F|.{18}$)/.test(id)
					? 'desktop'
					: 'unknown'

/** Upserts a receipt in the message */
export const updateMessageWithReceipt = (msg: Pick<WAMessage, 'userReceipt'>, receipt: MessageUserReceipt) => {
	msg.userReceipt = msg.userReceipt || []
	const recp = msg.userReceipt.find(m => m.userJid === receipt.userJid)
	if (recp) {
		Object.assign(recp, receipt)
	} else {
		msg.userReceipt.push(receipt)
	}
}

/** Update the message with a new reaction */
export const updateMessageWithReaction = (msg: Pick<WAMessage, 'reactions'>, reaction: proto.IReaction) => {
	const authorID = getKeyAuthor(reaction.key)

	const reactions = (msg.reactions || []).filter(r => getKeyAuthor(r.key) !== authorID)
	reaction.text = reaction.text || ''
	reactions.push(reaction)
	msg.reactions = reactions
}

/** Update the message with a new poll update */
export const updateMessageWithPollUpdate = (msg: Pick<WAMessage, 'pollUpdates'>, update: proto.IPollUpdate) => {
	const authorID = getKeyAuthor(update.pollUpdateMessageKey)

	const reactions = (msg.pollUpdates || []).filter(r => getKeyAuthor(r.pollUpdateMessageKey) !== authorID)
	if (update.vote?.selectedOptions?.length) {
		reactions.push(update)
	}

	msg.pollUpdates = reactions
}

/** Update the message with a new event response */
export const updateMessageWithEventResponse = (
	msg: Pick<WAMessage, 'eventResponses'>,
	update: proto.IEventResponse
) => {
	const authorID = getKeyAuthor(update.eventResponseMessageKey)

	const responses = (msg.eventResponses || []).filter(r => getKeyAuthor(r.eventResponseMessageKey) !== authorID)
	responses.push(update)

	msg.eventResponses = responses
}

type VoteAggregation = {
	name: string
	voters: string[]
}

/**
 * Aggregates all poll updates in a poll.
 * @param msg the poll creation message
 * @param meId your jid
 * @returns A list of options & their voters
 */
export function getAggregateVotesInPollMessage(
	{ message, pollUpdates }: Pick<WAMessage, 'pollUpdates' | 'message'>,
	meId?: string
) {
	const opts =
		message?.pollCreationMessage?.options ||
		message?.pollCreationMessageV2?.options ||
		message?.pollCreationMessageV3?.options ||
		[]
	const voteHashMap = opts.reduce(
		(acc, opt) => {
			const hash = sha256(Buffer.from(opt.optionName || '')).toString()
			acc[hash] = {
				name: opt.optionName || '',
				voters: []
			}
			return acc
		},
		{} as { [_: string]: VoteAggregation }
	)

	for (const update of pollUpdates || []) {
		const { vote } = update
		if (!vote) {
			continue
		}

		for (const option of vote.selectedOptions || []) {
			const hash = option.toString()
			let data = voteHashMap[hash]
			if (!data) {
				voteHashMap[hash] = {
					name: 'Unknown',
					voters: []
				}
				data = voteHashMap[hash]
			}

			voteHashMap[hash]!.voters.push(getKeyAuthor(update.pollUpdateMessageKey, meId))
		}
	}

	return Object.values(voteHashMap)
}

type ResponseAggregation = {
	response: string
	responders: string[]
}

/**
 * Aggregates all event responses in an event message.
 * @param msg the event creation message
 * @param meId your jid
 * @returns A list of response types & their responders
 */
export function getAggregateResponsesInEventMessage(
	{ eventResponses }: Pick<WAMessage, 'eventResponses'>,
	meId?: string
) {
	const responseTypes = ['GOING', 'NOT_GOING', 'MAYBE']
	const responseMap: { [_: string]: ResponseAggregation } = {}

	for (const type of responseTypes) {
		responseMap[type] = {
			response: type,
			responders: []
		}
	}

	for (const update of eventResponses || []) {
		const responseType = (update as any).eventResponse || 'UNKNOWN'
		if (responseType !== 'UNKNOWN' && responseMap[responseType]) {
			responseMap[responseType].responders.push(getKeyAuthor(update.eventResponseMessageKey, meId))
		}
	}

	return Object.values(responseMap)
}

/** Given a list of message keys, aggregates them by chat & sender. Useful for sending read receipts in bulk */
export const aggregateMessageKeysNotFromMe = (keys: WAMessageKey[]) => {
	const keyMap: { [id: string]: { jid: string; participant: string | undefined; messageIds: string[] } } = {}
	for (const { remoteJid, id, participant, fromMe } of keys) {
		if (!fromMe) {
			const uqKey = `${remoteJid}:${participant || ''}`
			if (!keyMap[uqKey]) {
				keyMap[uqKey] = {
					jid: remoteJid!,
					participant: participant!,
					messageIds: []
				}
			}

			keyMap[uqKey].messageIds.push(id!)
		}
	}

	return Object.values(keyMap)
}

type DownloadMediaMessageContext = {
	reuploadRequest: (msg: WAMessage) => Promise<WAMessage>
	logger: ILogger
}

const REUPLOAD_REQUIRED_STATUS = [410, 404]

/**
 * Downloads the given message. Throws an error if it's not a media message
 */
export const downloadMediaMessage = async <Type extends 'buffer' | 'stream'>(
	message: WAMessage,
	type: Type,
	options: MediaDownloadOptions,
	ctx?: DownloadMediaMessageContext
) => {
	const result = await downloadMsg().catch(async error => {
		if (
			ctx &&
			typeof error?.status === 'number' && // treat errors with status as HTTP failures requiring reupload
			REUPLOAD_REQUIRED_STATUS.includes(error.status as number)
		) {
			ctx.logger.info({ key: message.key }, 'sending reupload media request...')
			// request reupload
			message = await ctx.reuploadRequest(message)
			const result = await downloadMsg()
			return result
		}

		throw error
	})

	return result as Type extends 'buffer' ? Buffer : Transform

	async function downloadMsg() {
		const mContent = extractMessageContent(message.message)
		if (!mContent) {
			throw new Boom('No message present', { statusCode: 400, data: message })
		}

		const contentType = getContentType(mContent)
		let mediaType = contentType?.replace('Message', '') as MediaType
		const media = mContent[contentType!]

		if (!media || typeof media !== 'object' || (!('url' in media) && !('thumbnailDirectPath' in media))) {
			throw new Boom(`"${contentType}" message is not a media message`)
		}

		let download: DownloadableMessage
		if ('thumbnailDirectPath' in media && !('url' in media)) {
			download = {
				directPath: media.thumbnailDirectPath,
				mediaKey: media.mediaKey
			}
			mediaType = 'thumbnail-link'
		} else {
			download = media
		}

		const stream = await downloadContentFromMessage(download, mediaType, options)
		if (type === 'buffer') {
			const bufferArray: Buffer[] = []
			for await (const chunk of stream) {
				bufferArray.push(chunk)
			}

			return Buffer.concat(bufferArray)
		}

		return stream
	}
}

/** Checks whether the given message is a media message; if it is returns the inner content */
export const assertMediaContent = (content: proto.IMessage | null | undefined) => {
	content = extractMessageContent(content)
	const mediaContent =
		content?.documentMessage ||
		content?.imageMessage ||
		content?.videoMessage ||
		content?.audioMessage ||
		content?.stickerMessage
	if (!mediaContent) {
		throw new Boom('given message is not a media message', { statusCode: 400, data: content })
	}

	return mediaContent
}
