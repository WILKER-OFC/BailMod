import { Boom } from '@hapi/boom'
import { createHash } from 'crypto'
import { zipSync, type Zippable } from 'fflate'
import { promises as fs } from 'fs'
import { proto } from '../../WAProto/index.js'
import type { WAMediaUpload, WAMediaUploadFunction } from '../Types'
import { generateMessageIDV2, unixTimestampSeconds } from './generics'
import type { ILogger } from './logger'
import { encryptedStream, getImageProcessingLibrary } from './messages-media'

type MediaToBufferOptions = {
	maxSize?: number
	timeout?: number
}

export type StickerPackSticker = {
	media: WAMediaUpload
	emojis?: string[]
	accessibilityLabel?: string
}

export type StickerPackData = {
	name: string
	publisher: string
	stickers: StickerPackSticker[]
	cover: WAMediaUpload
	description?: string
}

export type PrepareStickerPackOptions = {
	upload: WAMediaUploadFunction
	logger?: ILogger
	options?: RequestInit
	mediaUploadTimeoutMs?: number
}

type UploadedEncryptedMedia = {
	fileSha256: Buffer
	fileEncSha256: Buffer
	mediaKey: Buffer
	directPath: string
	mediaKeyTimestamp: number
}

const getErrorMessage = (error: unknown) => (error instanceof Error ? error.message : String(error))

const isBoomError = (error: unknown): error is { output: { statusCode: number } } => {
	return (
		typeof error === 'object' &&
		error !== null &&
		'output' in error &&
		typeof (error as { output?: { statusCode?: unknown } }).output?.statusCode === 'number'
	)
}

const getErrorStatusCode = (error: unknown) => (isBoomError(error) ? error.output.statusCode : 500)

const isWebPBuffer = (buffer: Buffer) => {
	if (buffer.length < 12) {
		return false
	}

	const riffHeader = buffer.toString('ascii', 0, 4)
	const webpHeader = buffer.toString('ascii', 8, 12)

	return riffHeader === 'RIFF' && webpHeader === 'WEBP'
}

const isAnimatedWebP = (buffer: Buffer) => {
	if (!isWebPBuffer(buffer)) {
		return false
	}

	const maxChunkSize = 100 * 1024 * 1024
	const maxIterations = 1000

	let offset = 12
	let iterations = 0

	while (offset < buffer.length - 8 && iterations++ < maxIterations) {
		const chunkFourCC = buffer.toString('ascii', offset, offset + 4)
		const chunkSize = buffer.readUInt32LE(offset + 4)

		if (chunkSize > maxChunkSize) {
			return false
		}

		if (offset + 8 + chunkSize > buffer.length) {
			return false
		}

		if (chunkFourCC === 'VP8X' && offset + 8 < buffer.length) {
			const flags = buffer[offset + 8]
			if (flags && (flags & 0x02)) {
				return true
			}
		}

		if (chunkFourCC === 'ANIM' || chunkFourCC === 'ANMF') {
			return true
		}

		offset += 8 + chunkSize + (chunkSize % 2)
	}

	return false
}

const convertToWebP = async (buffer: Buffer, logger?: ILogger): Promise<{ webpBuffer: Buffer; isAnimated: boolean }> => {
	if (isWebPBuffer(buffer)) {
		const isAnimated = isAnimatedWebP(buffer)
		logger?.trace({ isAnimated }, 'input already WebP, preserving original buffer')
		return { webpBuffer: buffer, isAnimated }
	}

	const lib = await getImageProcessingLibrary()
	if (!('sharp' in lib) || !lib.sharp?.default) {
		throw new Boom(
			'Sharp library is required to convert non-WebP images to WebP format. Install with: yarn add sharp',
			{ statusCode: 400 }
		)
	}

	logger?.trace('converting image to WebP using sharp')
	const webpBuffer = await lib.sharp.default(buffer).webp().toBuffer()
	return { webpBuffer, isAnimated: false }
}

const generateSha256Hash = (buffer: Buffer) =>
	createHash('sha256').update(buffer).digest('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '')

const mediaToBuffer = async (media: WAMediaUpload, context: string, options?: MediaToBufferOptions): Promise<Buffer> => {
	const maxSize = options?.maxSize || 10 * 1024 * 1024
	const timeout = options?.timeout || 30000

	if (Buffer.isBuffer(media)) {
		if (media.length > maxSize) {
			throw new Boom(`${context} size (${(media.length / 1024).toFixed(2)}KB) exceeds ${maxSize / 1024}KB limit`, {
				statusCode: 413
			})
		}

		return media
	}

	if (typeof media === 'object' && media !== null && 'url' in media) {
		const url = media.url.toString()

		if (url.startsWith('data:')) {
			const base64Data = url.split(',')[1]
			if (!base64Data) {
				throw new Boom(`Invalid data URL for ${context}: missing base64 data`, { statusCode: 400 })
			}

			const buffer = Buffer.from(base64Data, 'base64')
			if (buffer.length > maxSize) {
				throw new Boom(
					`${context} data URL size (${(buffer.length / 1024).toFixed(2)}KB) exceeds ${maxSize / 1024}KB limit`,
					{ statusCode: 413 }
				)
			}

			return buffer
		}

		const controller = new AbortController()
		const timeoutId = setTimeout(() => controller.abort(), timeout)

		try {
			const response = await fetch(url, { signal: controller.signal })
			if (!response.ok) {
				throw new Boom(`Failed to download ${context} from URL: ${url}`, {
					statusCode: 400,
					data: { url, status: response.status }
				})
			}

			const contentLength = response.headers.get('content-length')
			if (contentLength && Number.parseInt(contentLength, 10) > maxSize) {
				throw new Boom(
					`${context} URL file size (${(Number.parseInt(contentLength, 10) / 1024).toFixed(2)}KB) exceeds ${
						maxSize / 1024
					}KB limit`,
					{ statusCode: 413, data: { url, contentLength } }
				)
			}

			if (!response.body) {
				throw new Boom(`Empty response body while downloading ${context} from URL: ${url}`, { statusCode: 400 })
			}

			const chunks: Uint8Array[] = []
			let totalSize = 0
			const reader = response.body.getReader()

			try {
				while (true) {
					const { done, value } = await reader.read()
					if (done) {
						break
					}

					if (!value) {
						continue
					}

					totalSize += value.length
					if (totalSize > maxSize) {
						throw new Boom(`${context} URL download exceeded ${maxSize / 1024}KB limit during transfer`, {
							statusCode: 413,
							data: { url, downloadedSize: totalSize }
						})
					}

					chunks.push(value)
				}
			} finally {
				reader.releaseLock()
			}

			return Buffer.concat(chunks.map(chunk => Buffer.from(chunk)))
		} catch (error) {
			if ((error as { name?: string })?.name === 'AbortError') {
				throw new Boom(`${context} URL download timeout (${timeout}ms)`, {
					statusCode: 408,
					data: { url, timeout }
				})
			}

			if (isBoomError(error)) {
				throw error
			}

			throw new Boom(`Failed to download ${context} from URL: ${getErrorMessage(error)}`, {
				statusCode: 400,
				data: { url }
			})
		} finally {
			clearTimeout(timeoutId)
		}
	}

	if (typeof media === 'object' && media !== null && 'stream' in media && media.stream) {
		const stream = media.stream
		return await new Promise<Buffer>((resolve, reject) => {
			const chunks: Buffer[] = []
			let totalSize = 0
			let timeoutId: NodeJS.Timeout | undefined

			const cleanup = () => {
				if (timeoutId) {
					clearTimeout(timeoutId)
				}
				stream.destroy()
			}

			timeoutId = setTimeout(() => {
				cleanup()
				reject(
					new Boom(`${context} stream read timeout (${timeout}ms)`, {
						statusCode: 408,
						data: { timeout }
					})
				)
			}, timeout)

			stream.on('data', (chunk: Buffer | Uint8Array | string) => {
				const part = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
				totalSize += part.length
				if (totalSize > maxSize) {
					cleanup()
					reject(
						new Boom(`${context} stream size exceeded ${maxSize / 1024}KB limit`, {
							statusCode: 413,
							data: { streamedSize: totalSize }
						})
					)
					return
				}

				chunks.push(part)
			})

			stream.on('end', () => {
				if (timeoutId) {
					clearTimeout(timeoutId)
				}
				resolve(Buffer.concat(chunks))
			})

			stream.on('error', (error: unknown) => {
				cleanup()
				reject(new Boom(`${context} stream error: ${getErrorMessage(error)}`, { statusCode: 400 }))
			})
		})
	}

	throw new Boom(`Invalid media type for ${context}`, { statusCode: 400 })
}

const uploadEncryptedMedia = async (
	media: WAMediaUpload,
	context: string,
	{
		upload,
		logger,
		options,
		mediaUploadTimeoutMs,
		mediaKey
	}: {
		upload: WAMediaUploadFunction
		logger?: ILogger
		options?: RequestInit
		mediaUploadTimeoutMs?: number
		mediaKey?: Buffer
	}
): Promise<UploadedEncryptedMedia> => {
	const { encFilePath, originalFilePath, fileEncSha256, fileSha256, mediaKey: finalMediaKey } = await encryptedStream(
		media,
		'sticker',
		{
			logger,
			opts: options,
			mediaKey
		}
	)

	try {
		const uploadResult = await upload(encFilePath, {
			mediaType: 'sticker',
			fileEncSha256B64: fileEncSha256.toString('base64'),
			timeoutMs: mediaUploadTimeoutMs
		})

		if (!uploadResult.directPath) {
			throw new Boom(`Upload response for ${context} did not include directPath`, {
				statusCode: 500,
				data: { uploadResult }
			})
		}

		return {
			fileSha256,
			fileEncSha256,
			mediaKey: finalMediaKey,
			directPath: uploadResult.directPath,
			mediaKeyTimestamp: typeof uploadResult.ts === 'number' ? uploadResult.ts : unixTimestampSeconds()
		}
	} finally {
		await fs.unlink(encFilePath).catch(() => {})
		if (originalFilePath) {
			await fs.unlink(originalFilePath).catch(() => {})
		}
	}
}

export const prepareStickerPackMessage = async (
	pack: StickerPackData,
	{ upload, logger, options, mediaUploadTimeoutMs }: PrepareStickerPackOptions
) => {
	const { name, publisher, stickers, cover, description } = pack

	if (!name || name.trim().length === 0) {
		throw new Boom('Sticker pack name is required', { statusCode: 400 })
	}

	if (!publisher || publisher.trim().length === 0) {
		throw new Boom('Publisher name is required', { statusCode: 400 })
	}

	if (!Array.isArray(stickers) || stickers.length === 0) {
		throw new Boom('At least one sticker is required', { statusCode: 400 })
	}

	if (stickers.length > 30) {
		throw new Boom(`Sticker pack cannot have more than 30 stickers (received ${stickers.length})`, {
			statusCode: 400
		})
	}

	if (!cover) {
		throw new Boom('Cover image is required', { statusCode: 400 })
	}

	logger?.info({ name, publisher, totalStickers: stickers.length }, 'starting sticker pack processing')

	const stickerPackId = generateMessageIDV2()
	const stickerData: Zippable = {}
	const stickerMetadata: proto.Message.StickerPackMessage.ISticker[] = []
	const metadataByHash = new Map<string, proto.Message.StickerPackMessage.ISticker>()

	const processedStickers = await Promise.all(
		stickers.map(async (sticker, i) => {
			try {
				const buffer = await mediaToBuffer(sticker.media, `sticker ${i + 1}`)
				const { webpBuffer, isAnimated } = await convertToWebP(buffer, logger)
				const recommendedLimit = isAnimated ? 1024 : 500
				const originalSizeKB = webpBuffer.length / 1024

				let finalWebpBuffer = webpBuffer
				if (originalSizeKB > recommendedLimit) {
					logger?.debug(
						{ index: i, originalSizeKB, recommendedLimit, isAnimated },
						`sticker ${i + 1} exceeds recommended size, attempting compression`
					)

					const lib = await getImageProcessingLibrary()
					if ('sharp' in lib && lib.sharp?.default && !isAnimated) {
						try {
							for (let quality = 85; quality >= 50; quality -= 10) {
								const compressed = await lib.sharp
									.default(webpBuffer)
									.webp({ quality, effort: 6 })
									.toBuffer()

								if (compressed.length / 1024 <= recommendedLimit) {
									finalWebpBuffer = compressed
									break
								}
							}
						} catch (compressionError) {
							logger?.warn(
								{ index: i, error: getErrorMessage(compressionError) },
								`failed to compress sticker ${i + 1}, using original`
							)
						}
					} else if (!isAnimated) {
						throw new Boom(
							`Sticker ${i + 1} size (${originalSizeKB.toFixed(2)}KB) exceeds recommended ${recommendedLimit}KB and ` +
								'sharp library is required for auto-compression. Install with: yarn add sharp',
							{ statusCode: 400 }
						)
					}
				}

				const fileName = `${generateSha256Hash(finalWebpBuffer)}.webp`
				return {
					fileName,
					webpBuffer: finalWebpBuffer,
					isAnimated,
					emojis: sticker.emojis || [],
					accessibilityLabel: sticker.accessibilityLabel
				}
			} catch (error) {
				throw new Boom(`Failed to process sticker ${i + 1}: ${getErrorMessage(error)}`, {
					statusCode: getErrorStatusCode(error),
					data: { stickerIndex: i, originalError: error }
				})
			}
		})
	)

	let duplicateCount = 0
	for (const result of processedStickers) {
		const { fileName, webpBuffer, isAnimated, emojis, accessibilityLabel } = result
		const existingMetadata = metadataByHash.get(fileName)

		if (existingMetadata) {
			duplicateCount++
			const mergedEmojis = Array.from(new Set([...(existingMetadata.emojis || []), ...emojis]))
			existingMetadata.emojis = mergedEmojis

			if (accessibilityLabel) {
				if (existingMetadata.accessibilityLabel) {
					existingMetadata.accessibilityLabel += ` / ${accessibilityLabel}`
				} else {
					existingMetadata.accessibilityLabel = accessibilityLabel
				}
			}
		} else {
			stickerData[fileName] = [new Uint8Array(webpBuffer), { level: 0 }]
			const metadata: proto.Message.StickerPackMessage.ISticker = {
				fileName,
				isAnimated,
				emojis,
				accessibilityLabel,
				isLottie: false,
				mimetype: 'image/webp'
			}
			metadataByHash.set(fileName, metadata)
			stickerMetadata.push(metadata)
		}
	}

	if (duplicateCount > 0) {
		logger?.info({ duplicateCount, uniqueStickers: stickerMetadata.length }, 'removed duplicate stickers')
	}

	let coverBuffer: Buffer
	let coverFileName: string
	try {
		coverBuffer = await mediaToBuffer(cover, 'cover image')
		const { webpBuffer: coverWebP } = await convertToWebP(coverBuffer, logger)
		coverFileName = `${stickerPackId}.webp`
		stickerData[coverFileName] = [new Uint8Array(coverWebP), { level: 0 }]
	} catch (error) {
		throw new Boom(`Failed to process cover image: ${getErrorMessage(error)}`, {
			statusCode: getErrorStatusCode(error),
			data: { originalError: error }
		})
	}

	let zipBuffer: Buffer
	let uniqueFiles: number
	try {
		uniqueFiles = Object.keys(stickerData).length
		zipBuffer = Buffer.from(zipSync(stickerData))
		const maxPackSize = 30 * 1024 * 1024
		if (zipBuffer.length > maxPackSize) {
			throw new Boom(
				`Total pack size exceeds ${maxPackSize / 1024 / 1024}MB limit. Current size: ${(
					zipBuffer.length /
					1024 /
					1024
				).toFixed(2)}MB.`,
				{ statusCode: 400 }
			)
		}
	} catch (error) {
		throw new Boom(`Failed to create ZIP archive: ${getErrorMessage(error)}`, {
			statusCode: getErrorStatusCode(error),
			data: { originalError: error }
		})
	}

	let stickerPackUpload: UploadedEncryptedMedia
	try {
		stickerPackUpload = await uploadEncryptedMedia(zipBuffer, 'sticker pack', {
			upload,
			logger,
			options,
			mediaUploadTimeoutMs
		})
	} catch (error) {
		throw new Boom(`Failed to upload sticker pack: ${getErrorMessage(error)}`, {
			statusCode: getErrorStatusCode(error),
			data: { originalError: error }
		})
	}

	let thumbnailBuffer: Buffer
	try {
		const lib = await getImageProcessingLibrary()
		if (!('sharp' in lib) || !lib.sharp?.default) {
			throw new Boom('Sharp library is required for thumbnail generation. Install with: yarn add sharp', {
				statusCode: 400
			})
		}

		thumbnailBuffer = await lib.sharp
			.default(coverBuffer)
			.resize(252, 252, { fit: 'cover', position: 'center' })
			.jpeg({ quality: 85 })
			.toBuffer()
	} catch (error) {
		throw new Boom(`Failed to generate thumbnail: ${getErrorMessage(error)}`, {
			statusCode: getErrorStatusCode(error),
			data: { originalError: error }
		})
	}

	let thumbUpload: UploadedEncryptedMedia
	try {
		thumbUpload = await uploadEncryptedMedia(thumbnailBuffer, 'sticker pack thumbnail', {
			upload,
			logger,
			options,
			mediaUploadTimeoutMs,
			mediaKey: stickerPackUpload.mediaKey
		})
	} catch (error) {
		throw new Boom(`Failed to upload thumbnail: ${getErrorMessage(error)}`, {
			statusCode: getErrorStatusCode(error),
			data: { originalError: error }
		})
	}

	logger?.info(
		{
			packId: stickerPackId,
			totalStickers: stickers.length,
			uniqueFiles: uniqueFiles - 1,
			zipSizeKB: (zipBuffer.length / 1024).toFixed(2)
		},
		'sticker pack message prepared successfully'
	)

	return proto.Message.StickerPackMessage.create({
		stickerPackId,
		name,
		publisher,
		packDescription: description,
		stickerPackOrigin: proto.Message.StickerPackMessage.StickerPackOrigin.USER_CREATED,
		stickerPackSize: zipBuffer.length,
		stickers: stickerMetadata,
		fileSha256: stickerPackUpload.fileSha256,
		fileEncSha256: stickerPackUpload.fileEncSha256,
		mediaKey: stickerPackUpload.mediaKey,
		directPath: stickerPackUpload.directPath,
		fileLength: zipBuffer.length,
		mediaKeyTimestamp: stickerPackUpload.mediaKeyTimestamp,
		trayIconFileName: coverFileName,
		thumbnailDirectPath: thumbUpload.directPath,
		thumbnailSha256: createHash('sha256').update(thumbnailBuffer).digest(),
		thumbnailEncSha256: thumbUpload.fileEncSha256,
		thumbnailHeight: 252,
		thumbnailWidth: 252,
		imageDataHash: createHash('sha256').update(thumbnailBuffer).digest('base64')
	})
}
