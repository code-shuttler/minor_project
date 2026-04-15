import client from '@/shared/api/client'
import axios, { AxiosError } from 'axios'

export type ApiError = {
  message: string
  status?: number
  data?: unknown
}

const handleError = (error: unknown): never => {
  if (axios.isAxiosError(error)) {
    const axiosError = error as AxiosError<{ message?: string }>
    const errorMessage = axiosError.response?.data?.message || axiosError.message || 'An error occurred'
    const apiError: ApiError = {
      message: errorMessage,
      status: axiosError.response?.status,
      data: axiosError.response?.data,
    }
    throw apiError
  }
  throw { message: 'Unknown error occurred' } as ApiError
}

export const get = async <T>(url: string): Promise<T> => {
  try {
    const res = await client.get<T>(url)
    return res.data
  } catch (error) {
    return handleError(error)
  }
}

export const post = async <T, D>(url: string, data: D): Promise<T> => {
  try {
    const res = await client.post<T>(url, data)
    return res.data
  } catch (error) {
    return handleError(error)
  }
}

export const put = async <T, D>(url: string, data: D): Promise<T> => {
  try {
    const res = await client.put<T>(url, data)
    return res.data
  } catch (error) {
    return handleError(error)
  }
}

export const del = async <T>(url: string): Promise<T> => {
  try {
    const res = await client.delete<T>(url)
    return res.data
  } catch (error) {
    return handleError(error)
  }
}

