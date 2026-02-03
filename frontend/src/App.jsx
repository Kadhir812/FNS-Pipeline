import React, { useState } from 'react';
import Dashboard from './pages/Dashboard';
import PerformanceMetrics from './components/charts/PerformanceMetrics';
import { ThemeProvider } from './contexts/ThemeContext';
import './App.css';
import './components/EntityDisplay.css';

const App = () => {
  const [currentPage, setCurrentPage] = useState('dashboard'); // 'dashboard' | 'metrics'

  return (
    <ThemeProvider>
      <div className="app-container">
        <nav className="app-nav">
          <button 
            className={`nav-btn ${currentPage === 'dashboard' ? 'active' : ''}`}
            onClick={() => setCurrentPage('dashboard')}
          >
            Dashboard
          </button>
          <button 
            className={`nav-btn ${currentPage === 'metrics' ? 'active' : ''}`}
            onClick={() => setCurrentPage('metrics')}
          >
            Metrics
          </button>
        </nav>
        {currentPage === 'dashboard' && <Dashboard />}
        {currentPage === 'metrics' && <PerformanceMetrics />}
      </div>
    </ThemeProvider>
  );
};

export default App;
