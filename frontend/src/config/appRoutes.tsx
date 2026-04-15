import { Routes, Route, Outlet } from 'react-router-dom'
import Login from '@/features/auth/pages/login';
import Signup from '@/features/auth/pages/signup';
import ProtectedRoutes from '@/config/protectedRoutes';
import Home from '@/features/home';
import gradientBackgroundDark from "@/assets/images/gradient-background-dark.png";
import gradientBackgroundLight from "@/assets/images/gradient-background-light.png";
import Header from "@/shared/components/header";
import Departments from '@/features/departments';
import Subjects from '@/features/subject/index';
import Rooms from '@/features/room';
import TimeTable from '@/features/timetable';
import TimeSlotPage from '@/features/timeslot';

function ProtectedLayout() {
  const isDarkMode = typeof document !== "undefined" && document.documentElement.classList.contains("dark");

  return (
    <div className="min-h-screen flex flex-wrap items-center justify-center p-4"
      style={{
        backgroundImage: `url(${isDarkMode ? gradientBackgroundDark : gradientBackgroundLight})`,
        backgroundSize: "cover",
        backgroundPosition: "center", 
        backgroundRepeat: "no-repeat",
      }}>
      <Header />
      <Outlet />
    </div>
  )
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/signup" element={<Signup />} />
      <Route element={<ProtectedRoutes />}>
        <Route element={<ProtectedLayout />}>
            <Route path="/" element={<Home />} />
            <Route path="/departments" element={<Departments />} />
            <Route path="/subjects" element={<Subjects />} />
            <Route path="/rooms" element={<Rooms />} />
            <Route path="/timeslots" element={<TimeSlotPage />} />
            <Route path="/timetable" element={<TimeTable />} />
        </Route>
      </Route>
      <Route path="*" element={<h1>404 Not Found</h1>} />
    </Routes>

  )
}

export default AppRoutes;