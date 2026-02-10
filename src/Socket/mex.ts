import { Boom } from '@hapi/boom'
import type { BinaryNode } from '../WABinary'
import { getBinaryNodeChild, S_WHATSAPP_NET } from '../WABinary'

const wMexQuery = (
	variables: Record<string, unknown>,
	queryId: string,
	query: (node: BinaryNode) => Promise<BinaryNode>,
	generateMessageTag: () => string
) => {
	return query({
		tag: 'iq',
		attrs: {
			id: generateMessageTag(),
			type: 'get',
			to: S_WHATSAPP_NET,
			xmlns: 'w:mex'
		},
		content: [
			{
				tag: 'query',
				attrs: { query_id: queryId },
				content: Buffer.from(JSON.stringify({ variables }), 'utf-8')
			}
		]
	})
}

export const executeWMexQuery = async <T>(
	variables: Record<string, unknown>,
	queryId: string,
	dataPath: string,
	query: (node: BinaryNode) => Promise<BinaryNode>,
	generateMessageTag: () => string
): Promise<T> => {
	const result = await wMexQuery(variables, queryId, query, generateMessageTag)
	const child = getBinaryNodeChild(result, 'result')
	if (child?.content) {
		const data = JSON.parse(child.content.toString())

		if (Array.isArray(data.errors) && data.errors.length > 0) {
			const errorMessages = data.errors.map((err: any) => err?.message || 'Unknown error').join(', ')
			const firstError = data.errors[0]
			const errorCode = firstError?.extensions?.error_code || 400
			throw new Boom(`GraphQL server error: ${errorMessages}`, { statusCode: errorCode, data: firstError })
		}

		const response = dataPath ? data?.data?.[dataPath] : data?.data
		if (typeof response !== 'undefined') {
			return response as T
		}
	}

	const action = (dataPath || '').startsWith('xwa2_')
		? dataPath.substring(5).replace(/_/g, ' ')
		: dataPath?.replace(/_/g, ' ')
	throw new Boom(`Failed to ${action}, unexpected response structure.`, { statusCode: 400, data: result })
}

/**
 * Execute a WMex query where the response data path is unstable or not needed.
 * Parses and throws GraphQL errors (if present) but does not validate/require a `dataPath`.
 *
 * This matches libraries that "fire and forget" mex mutations (eg. newsletter follow/unfollow/mute).
 */
export const executeWMexQueryIgnoreResponse = async (
	variables: Record<string, unknown>,
	queryId: string,
	query: (node: BinaryNode) => Promise<BinaryNode>,
	generateMessageTag: () => string
): Promise<void> => {
	const result = await wMexQuery(variables, queryId, query, generateMessageTag)
	const child = getBinaryNodeChild(result, 'result')
	if (!child?.content) {
		// Some mex mutations can return an empty result; treat as success.
		return
	}

	let data: any
	try {
		data = JSON.parse(child.content.toString())
	} catch {
		// If parsing fails, we still consider it a successful fire-and-forget mex mutation.
		// This mirrors other libs that don't require a stable response shape for these operations.
		return
	}

	if (Array.isArray(data.errors) && data.errors.length > 0) {
		const errorMessages = data.errors.map((err: any) => err?.message || 'Unknown error').join(', ')
		const firstError = data.errors[0]
		const errorCode = firstError?.extensions?.error_code || 400
		throw new Boom(`GraphQL server error: ${errorMessages}`, { statusCode: errorCode, data: firstError })
	}
}
