import { del, get, post, put } from "@/shared/api/http";

export type Teacher = {
  id: number;
  name: string;
  abbreviation: string;
  specializationIds: number[];
  departmentId: number;
};

export type TeacherPayload = {
  name: string;
  abbreviation: string;
  specializationIds: number[];
  departmentId: number;
};

export const listTeachers = async (): Promise<Teacher[]> => {
  return get<Teacher[]>("/teachers");
};

export const createTeacher = async (payload: TeacherPayload): Promise<Teacher> => {
  return post<Teacher, TeacherPayload>("/teachers", payload);
};

export const updateTeacher = async (id: number, payload: TeacherPayload): Promise<Teacher> => {
  return put<Teacher, TeacherPayload>(`/teachers/${id}`, payload);
};

export const deleteTeacher = async (id: number): Promise<void> => {
  return del<void>(`/teachers/${id}`);
};
