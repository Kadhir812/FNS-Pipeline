import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Activity, Clock, TrendingUp, RefreshCw } from 'lucide-react';
import './PerformanceMetrics.css';

const PerformanceMetrics = () => {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [view, setView] = useState('throughput'); // 'throughput' or 'latency'
  const [autoRefresh, setAutoRefresh] = useState(true);

  const fetchMetrics = async () => {
    try {
      setError(null);
      const response = await fetch('/api/v1/metrics/performance');
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data = await response.json();
      if (data.success) {
        setMetrics(data.data);
      } else {
        throw new Error(data.message || 'Failed to fetch metrics');
      }
    } catch (err) {
      console.error('Metrics fetch error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
  }, []);

  useEffect(() => {
    if (!autoRefresh) return;
    
    const interval = setInterval(() => {
      fetchMetrics();
    }, 30000); // Refresh every 30 seconds

    return () => clearInterval(interval);
  }, [autoRefresh]);

  const pipelineColors = {
    extract: '#8b5cf6',
    transform: '#ec4899',
    backend: '#3b82f6',
    baseline: '#10b981'
  };

  if (loading && !metrics) {
    return (
      <div className="performance-metrics">
        <div className="metrics-loading">
          <RefreshCw className="spin" size={32} />
          <p>Loading performance metrics...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="performance-metrics">
        <div className="metrics-error">
          <p>Error loading metrics: {error}</p>
          <button onClick={fetchMetrics} className="retry-btn">
            <RefreshCw size={16} />
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="performance-metrics">
      <div className="metrics-header">
        <div className="metrics-title">
          <Activity size={28} />
          <h1>Pipeline Performance Metrics</h1>
        </div>
        <div className="metrics-controls">
          <label className="auto-refresh-toggle">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
            />
            Auto-refresh (30s)
          </label>
          <button onClick={fetchMetrics} className="refresh-btn" title="Refresh now">
            <RefreshCw size={16} className={loading ? 'spin' : ''} />
          </button>
        </div>
      </div>

      <div className="metrics-view-toggle">
        <button
          className={`toggle-btn ${view === 'throughput' ? 'active' : ''}`}
          onClick={() => setView('throughput')}
        >
          <TrendingUp size={18} />
          Throughput
        </button>
        <button
          className={`toggle-btn ${view === 'latency' ? 'active' : ''}`}
          onClick={() => setView('latency')}
        >
          <Clock size={18} />
          Latency
        </button>
      </div>

      {view === 'throughput' && metrics?.throughput && (
        <div className="metrics-chart-container">
          <h2>Throughput (Articles/Min)</h2>
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={metrics.throughput} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="label" stroke="#9ca3af" />
              <YAxis stroke="#9ca3af" label={{ value: 'Articles/Min', angle: -90, position: 'insideLeft' }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Legend />
              <Bar dataKey="value" name="Throughput" fill="#3b82f6" radius={[8, 8, 0, 0]}>
                {metrics.throughput.map((entry, index) => (
                  <Bar key={`bar-${index}`} fill={pipelineColors[entry.pipeline] || '#3b82f6'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {view === 'latency' && metrics?.latency && (
        <>
          <div className="metrics-chart-container">
            <h2>Latency Percentiles (ms)</h2>
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={metrics.latency} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="label" stroke="#9ca3af" />
                <YAxis stroke="#9ca3af" label={{ value: 'Latency (ms)', angle: -90, position: 'insideLeft' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                  labelStyle={{ color: '#f3f4f6' }}
                />
                <Legend />
                <Bar dataKey="p50" name="p50" fill="#10b981" radius={[4, 4, 0, 0]} />
                <Bar dataKey="p95" name="p95" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                <Bar dataKey="p99" name="p99" fill="#ef4444" radius={[4, 4, 0, 0]} />
                <Bar dataKey="avg" name="Average" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="latency-cards">
            {metrics.latency.map((pipeline) => (
              <div key={pipeline.pipeline} className="latency-card" style={{ borderTopColor: pipelineColors[pipeline.pipeline] }}>
                <h3>{pipeline.label}</h3>
                <div className="latency-stats">
                  <div className="stat">
                    <span className="stat-label">p50</span>
                    <span className="stat-value">{pipeline.p50.toFixed(1)}ms</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">p95</span>
                    <span className="stat-value">{pipeline.p95.toFixed(1)}ms</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">p99</span>
                    <span className="stat-value">{pipeline.p99.toFixed(1)}ms</span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">Avg</span>
                    <span className="stat-value">{pipeline.avg.toFixed(1)}ms</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
};

export default PerformanceMetrics;
