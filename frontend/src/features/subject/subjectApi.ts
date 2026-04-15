import { del, get, post, put } from "@/shared/api/http";

export type Subject = {
  id: number;
  name: string;
  department_ids: number[];
  lecture: number;
  tutorial: number;
  practical: number;
};

export type SubjectPayload = {
  name: string;
  department_ids: number[];
  lecture: number;
  tutorial: number;
  practical: number;
};

type SubjectApiModel = {
  id: number;
  name: string;
  department_ids?: number[];
  departmentIds?: number[];
  lecture: number;
  tutorial: number;
  practical: number;
};

const normalizeSubject = (subject: SubjectApiModel): Subject => ({
  id: subject.id,
  name: subject.name,
  department_ids: subject.department_ids ?? subject.departmentIds ?? [],
  lecture: subject.lecture,
  tutorial: subject.tutorial,
  practical: subject.practical,
});

export const listSubjects = async (): Promise<Subject[]> => {
  const response = await get<SubjectApiModel[]>("/subjects");
  return response.map(normalizeSubject);
};

export const createSubject = async (payload: SubjectPayload): Promise<Subject> => {
  const response = await post<SubjectApiModel, SubjectPayload>("/subjects", payload);
  return normalizeSubject(response);
};

export const updateSubject = async (id: number, payload: SubjectPayload): Promise<Subject> => {
  const response = await put<SubjectApiModel, SubjectPayload>(`/subjects/${id}`, payload);
  return normalizeSubject(response);
};

export const deleteSubject = async (id: number): Promise<void> => {
  return del<void>(`/subjects/${id}`);
};
