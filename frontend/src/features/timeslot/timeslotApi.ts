import { del, get, post, put } from "@/shared/api/http";

export type TimeSlot = {
  id: number;
  day: string;
  start_time: string;
  end_time: string;
};

export type TimeSlotPayload = {
  day: string;
  start_time: string;
  end_time: string;
};

type TimeSlotApiModel = {
  id: number;
  day: string;
  start_time?: string;
  startTime?: string;
  end_time?: string;
  endTime?: string;
};

const normalizeTimeSlot = (timeSlot: TimeSlotApiModel): TimeSlot => ({
  id: timeSlot.id,
  day: timeSlot.day,
  start_time: timeSlot.start_time ?? timeSlot.startTime ?? "",
  end_time: timeSlot.end_time ?? timeSlot.endTime ?? "",
});

export const listTimeSlots = async (): Promise<TimeSlot[]> => {
  const response = await get<TimeSlotApiModel[]>("/time-slots");
  return response.map(normalizeTimeSlot);
};

export const createTimeSlot = async (payload: TimeSlotPayload): Promise<TimeSlot> => {
  const response = await post<TimeSlotApiModel, TimeSlotPayload>("/time-slots", payload);
  return normalizeTimeSlot(response);
};

export const updateTimeSlot = async (id: number, payload: TimeSlotPayload): Promise<TimeSlot> => {
  const response = await put<TimeSlotApiModel, TimeSlotPayload>(`/time-slots/${id}`, payload);
  return normalizeTimeSlot(response);
};

export const deleteTimeSlot = async (id: number): Promise<void> => {
  return del<void>(`/time-slots/${id}`);
};