import { del, get, post, put } from "@/shared/api/http";

export type Timetable = {
  id: number;
  batch_id: number;
  subject_id: number;
  teacher_id: number;
  time_slot_id: number;
  room_id: number;
};

export type TimetablePayload = {
  batch_id: number;
  subject_id: number;
  teacher_id: number;
  time_slot_id: number;
};

export type TimetableAutoGeneratePayload = {
  batches_id: number[];
};

type TimetableApiModel = {
  id: number;
  batch_id?: number;
  batchId?: number;
  subject_id?: number;
  subjectId?: number;
  teacher_id?: number;
  teacherId?: number;
  time_slot_id?: number;
  timeSlotId?: number;
  room_id?: number;
  roomId?: number;
};

const normalizeTimetable = (timetable: TimetableApiModel): Timetable => ({
  id: timetable.id,
  batch_id: timetable.batch_id ?? timetable.batchId ?? 0,
  subject_id: timetable.subject_id ?? timetable.subjectId ?? 0,
  teacher_id: timetable.teacher_id ?? timetable.teacherId ?? 0,
  time_slot_id: timetable.time_slot_id ?? timetable.timeSlotId ?? 0,
  room_id: timetable.room_id ?? timetable.roomId ?? 0,
});

export type TimeSlot = {
  id: number;
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

export const listTimetables = async (): Promise<Timetable[]> => {
  const response = await get<TimetableApiModel[]>("/timetables");
  return response.map(normalizeTimetable);
};

export const createTimetable = async (payload: TimetablePayload): Promise<Timetable> => {
  const response = await post<TimetableApiModel, TimetablePayload>("/timetables", payload);
  return normalizeTimetable(response);
};

export const updateTimetable = async (id: number, payload: TimetablePayload): Promise<Timetable> => {
  const response = await put<TimetableApiModel, TimetablePayload>(`/timetables/${id}`, payload);
  return normalizeTimetable(response);
};

export const deleteTimetable = async (id: number): Promise<void> => {
  return del<void>(`/timetables/${id}`);
};

export const autoGenerateTimetables = async (
  payload: TimetableAutoGeneratePayload
): Promise<Timetable[]> => {
  const response = await post<TimetableApiModel[], TimetableAutoGeneratePayload>(
    "/timetables",
    payload
  );
  return response.map(normalizeTimetable);
};

export const listTimeSlots = async (): Promise<TimeSlot[]> => {
  const response = await get<TimeSlotApiModel[]>("/time-slots");
  return response.map(normalizeTimeSlot);
};