import React from 'react';
import Dashboard from './pages/Dashboard';
import { ThemeProvider } from './contexts/ThemeContext';
import './App.css';
import './components/EntityDisplay.css';

const App = () => {
  return (
    <ThemeProvider>
      <Dashboard />
    </ThemeProvider>
  );
};

export default App;
