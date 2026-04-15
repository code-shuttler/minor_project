import { Link } from 'react-router-dom';
import { Avatar, AvatarFallback } from './ui/avatar';
import { useEffect, useState } from 'react';
import { authContext } from '@/features/auth/authContext';

function Header() {
    const [User, setUser] = useState<{ username: string } | null>(null);
    useEffect(() => {
        const loadCurrentUser = async () => {
            const user = await authContext.getCurrentUser();
            setUser(user);
        };

        void loadCurrentUser();
    }, []);
    return (
        <>
            <header className="min-w-[calc(100vw-2rem)] fixed top-4 z-10 rounded-2xl bg-white/20 shadow-lg border border-black/10">
                <nav className="flex items-center justify-between p-4">
                    <div className="text-xl font-bold">
                        <Link to="/">STMS</Link>
                    </div>
                    <div className="space-x-4">
                        <Link to="/" className="hover:text-gray-700">Home</Link>
                        <Link to="/departments" className="hover:text-gray-700">Departments</Link>
                        <Link to="/subjects" className="hover:text-gray-700">Subjects</Link>
                        <Link to="/rooms" className="hover:text-gray-700">Rooms</Link>
                        <Link to="/timeslots" className="hover:text-gray-700">Time Slots</Link>
                        <Link to="/timetable" className="hover:text-gray-700">TimeTable</Link>
                    </div>
                    <div className="flex items-center space-x-4">
                        <Avatar >
                            <AvatarFallback className="bg-gray-300/50 text-gray-600 border border-gray-300 shadow-xl">{User?.username?.charAt(0).toUpperCase() ?? 'U'}</AvatarFallback>
                        </Avatar>
                    </div>
                </nav>
            </header>
        </>
    );
}

export default Header;