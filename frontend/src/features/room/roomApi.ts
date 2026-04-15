import { del, get, post, put } from "@/shared/api/http";

export type Room = {
  id: number;
  name: string;
  isLab: boolean;
  departmentId: number | null;
};

export type RoomPayload = {
  name: string;
  isLab: boolean;
  departmentId: number | null;
};

type RoomApiModel = {
  id: number;
  name: string;
  isLab?: boolean;
  islab?: boolean;
  department_id?: number | null;
  departmentId?: number | null;
};

const normalizeRoom = (room: RoomApiModel): Room => ({
  id: room.id,
  name: room.name,
  isLab: room.isLab ?? room.islab ?? false,
  departmentId: room.department_id ?? room.departmentId ?? null,
});

export const listRooms = async (): Promise<Room[]> => {
  const response = await get<RoomApiModel[]>('/rooms');
  return response.map(normalizeRoom);
};

export const createRoom = async (payload: RoomPayload): Promise<Room> => {
  const response = await post<RoomApiModel, RoomPayload>('/rooms', payload);
  return normalizeRoom(response);
};

export const updateRoom = async (id: number, payload: RoomPayload): Promise<Room> => {
  const response = await put<RoomApiModel, RoomPayload>(`/rooms/${id}`, payload);
  return normalizeRoom(response);
};

export const deleteRoom = async (id: number): Promise<void> => {
  return del<void>(`/rooms/${id}`);
};