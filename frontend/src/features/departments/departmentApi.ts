import { del, get, post, put } from "@/shared/api/http";

export type Department = {
  id: number;
  name: string;
  abbreviation: string;
};

export type DepartmentPayload = {
  name: string;
  abbreviation: string;
};

export const listDepartments = async (): Promise<Department[]> => {
  return get<Department[]>("/departments");
};

export const createDepartment = async (payload: DepartmentPayload): Promise<Department> => {
  return post<Department, DepartmentPayload>("/departments", payload);
};

export const updateDepartment = async (id: number, payload: DepartmentPayload): Promise<Department> => {
  return put<Department, DepartmentPayload>(`/departments/${id}`, payload);
};

export const deleteDepartment = async (id: number): Promise<void> => {
  return del<void>(`/departments/${id}`);
};
