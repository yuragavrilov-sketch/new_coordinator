/**
 * Typed HTTP client for backend API calls.
 * Replaces scattered fetch() calls across components.
 */

class ApiError extends Error {
  constructor(
    public status: number,
    public statusText: string,
    public body: unknown,
  ) {
    super(`API error ${status}: ${statusText}`);
    this.name = "ApiError";
  }
}

async function handleResponse<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    let body: unknown;
    try {
      body = await resp.json();
    } catch {
      body = await resp.text();
    }
    throw new ApiError(resp.status, resp.statusText, body);
  }
  const text = await resp.text();
  return text ? JSON.parse(text) : (undefined as T);
}

class ApiClient {
  constructor(private baseUrl: string = "") {}

  async get<T>(path: string, signal?: AbortSignal): Promise<T> {
    const resp = await fetch(`${this.baseUrl}${path}`, { signal });
    return handleResponse<T>(resp);
  }

  async post<T>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    const resp = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal,
    });
    return handleResponse<T>(resp);
  }

  async patch<T>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    const resp = await fetch(`${this.baseUrl}${path}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal,
    });
    return handleResponse<T>(resp);
  }

  async delete<T = void>(path: string, signal?: AbortSignal): Promise<T> {
    const resp = await fetch(`${this.baseUrl}${path}`, {
      method: "DELETE",
      signal,
    });
    return handleResponse<T>(resp);
  }
}

export const api = new ApiClient();
export { ApiError };
