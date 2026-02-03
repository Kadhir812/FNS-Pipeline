import React, { useEffect, useState } from 'react';
import { ResponsiveContainer, Tooltip, Area, AreaChart } from 'recharts';
import { motion } from 'framer-motion';
import { fetchArticleHistory } from '../../services/api';

const ArticleMiniChart = ({ article, relatedArticles = [], height = 70 }) => {
  const [chartData, setChartData] = useState([]);
  const [loading, setLoading] = useState(false);

  const getSymbol = () => {
    if (!article) return null;
    if (article.symbol) return article.symbol;
    if (article.symbols?.length) return article.symbols[0];
    if (article.displaySymbol) return article.displaySymbol;
    if (article.ticker) return article.ticker;
    return null;
  };

  useEffect(() => {
    let mounted = true;
    const fetchHistory = async () => {
      setLoading(true);
      const symbol = getSymbol();
      if (!symbol) return setLoading(false);

      try {
        const rows = await fetchArticleHistory(symbol, 200);
        if (!mounted) return;
        if (rows?.length) {
          const points = rows.map((r, idx) => ({
            index: idx,
            score: Number(r.final_score),
            ts: r.ts,
          }));
          setChartData(points);
        }
      } catch (err) {
        console.warn('Chart fetch error:', err);
      } finally {
        if (mounted) setLoading(false);
      }
    };
    fetchHistory();
    return () => (mounted = false);
  }, [article]);

  const getLineColor = (data) => {
    const arr = data?.length ? data : chartData;
    if (!arr?.length) return '#9CA3AF';
    const avg = arr.reduce((s, d) => s + d.score, 0) / arr.length;
    if (avg > 0.5) return '#22C55E'; // emerald
    if (avg < -0.5) return '#EF4444'; // red
    return '#06B6D4'; // cyan
  };

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload?.length) {
      return (
        <div
          style={{
            background: 'rgba(17, 24, 39, 0.8)',
            color: 'white',
            fontSize: '12px',
            padding: '4px 8px',
            borderRadius: '6px',
            boxShadow: '0 2px 6px rgba(0,0,0,0.2)',
          }}
        >
          <p style={{ margin: 0 }}>Score: {payload[0].value.toFixed(2)}</p>
        </div>
      );
    }
    return null;
  };

  const fallbackData = (() => {
    if (chartData?.length) return chartData;
    if (relatedArticles?.length) {
      const sorted = [...relatedArticles]
        .filter((a) => a.publishedAt || a.published_date || a.timestamp)
        .sort(
          (a, b) =>
            new Date(a.publishedAt || a.published_date || a.timestamp) -
            new Date(b.publishedAt || b.published_date || b.timestamp)
        )
        .slice(-10);
      return sorted.map((a, i) => ({
        index: i,
        score: parseFloat(a.final_score) || 0,
      }));
    }

    const base = parseFloat(article.final_score) || 0;
    const sentiment = parseFloat(article.sentiment) || 0;
    const data = [];
    for (let i = 0; i < 8; i++) {
      const variance = (Math.random() - 0.5) * 0.25;
      const trend = sentiment > 0 ? 0.05 : -0.05;
      data.push({ index: i, score: base + i * trend + variance });
    }
    return data;
  })();

  const color = getLineColor(fallbackData);

  return (
    <motion.div
      className="article-mini-chart"
      initial={{ opacity: 0, y: 5 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
      style={{
        width: '100%',
        height,
        position: 'relative',
      }}
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={fallbackData}>
          <defs>
            <linearGradient id="colorTrend" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.5} />
              <stop offset="100%" stopColor={color} stopOpacity={0.05} />
            </linearGradient>
          </defs>
          <Area
            type="monotone"
            dataKey="score"
            stroke={color}
            fill="url(#colorTrend)"
            strokeWidth={2.5}
            dot={false}
            isAnimationActive={true}
            animationDuration={700}
          />
          <Tooltip content={<CustomTooltip />} />
        </AreaChart>
      </ResponsiveContainer>

      {/* Optional average indicator */}
      <div
        style={{
          position: 'absolute',
          top: 0,
          right: 8,
          fontSize: 10,
          color: color,
        }}
      >
        {loading ? '...' : fallbackData.length ? '⟶' : '–'}
      </div>
    </motion.div>
  );
};

export default ArticleMiniChart;
