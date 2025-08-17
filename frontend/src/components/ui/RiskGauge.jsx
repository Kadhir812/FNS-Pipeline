import React from 'react';

const RiskGauge = ({ score }) => (
  <div className="risk-gauge">
    <div className="risk-gauge-track">
      <div 
        className="risk-gauge-fill" 
        style={{ width: `${score * 100}%` }}
      />
    </div>
    <span className="risk-gauge-label">{(score * 100).toFixed(0)}%</span>
  </div>
);

export default RiskGauge;
