/**
 * Centralized API client for making HTTP requests.
 */

const normalizeApiBase = (baseUrl) => {
  if (!baseUrl) return '/api'
  const trimmed = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl
  if (trimmed.startsWith('http')) {
    return trimmed.endsWith('/api') ? trimmed : `${trimmed}/api`
  }
  return trimmed
}

const API_BASE_URL = normalizeApiBase(import.meta.env.VITE_API_BASE_URL || '/api')

/**
 * Base fetch wrapper with error handling.
 */
async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  })

  if (!response.ok) {
    const errorText = await response.text()
    let errorMessage = `HTTP ${response.status}: ${response.statusText}`

    try {
      const errorData = JSON.parse(errorText)
      errorMessage = errorData.detail || errorData.message || errorMessage
    } catch {
      errorMessage = errorText || errorMessage
    }

    throw new Error(errorMessage)
  }

  // Handle 204 No Content
  if (response.status === 204) {
    return null
  }

  return response.json()
}

/**
 * API methods
 */
export const api = {
  /**
   * GET request
   */
  get: async (endpoint) => {
    return fetchJSON(`${API_BASE_URL}${endpoint}`, {
      method: 'GET',
    })
  },

  /**
   * POST request
   */
  post: async (endpoint, data) => {
    return fetchJSON(`${API_BASE_URL}${endpoint}`, {
      method: 'POST',
      body: JSON.stringify(data),
    })
  },

  /**
   * PUT request
   */
  put: async (endpoint, data) => {
    return fetchJSON(`${API_BASE_URL}${endpoint}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    })
  },

  /**
   * DELETE request
   */
  delete: async (endpoint) => {
    return fetchJSON(`${API_BASE_URL}${endpoint}`, {
      method: 'DELETE',
    })
  },
}
