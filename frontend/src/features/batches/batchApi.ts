import { del, get, post, put } from "@/shared/api/http";

export type Batch = {
  id: number;
  name: string;
  semester: number;
  department_id: number;
  subject_ids: number[];
};

export type BatchPayload = {
  name: string;
  semester: number;
  department_id: number;
  subject_ids: number[];
};

type BatchApiModel = {
  id: number;
  name: string;
  semester: number;
  department_id: number;
  subject_ids?: number[];
  subjectIds?: number[];
};

const normalizeBatch = (batch: BatchApiModel): Batch => ({
  id: batch.id,
  name: batch.name,
  semester: batch.semester,
  department_id: batch.department_id,
  subject_ids: batch.subject_ids ?? batch.subjectIds ?? [],
});

export const listBatches = async (): Promise<Batch[]> => {
  const response = await get<BatchApiModel[]>("/batches");
  return response.map(normalizeBatch);
};

export const createBatch = async (payload: BatchPayload): Promise<Batch> => {
  const response = await post<BatchApiModel, BatchPayload>("/batches", payload);
  return normalizeBatch(response);
};

export const updateBatch = async (id: number, payload: BatchPayload): Promise<Batch> => {
  const response = await put<BatchApiModel, BatchPayload>(`/batches/${id}`, payload);
  return normalizeBatch(response);
};

export const deleteBatch = async (id: number): Promise<void> => {
  return del<void>(`/batches/${id}`);
};
