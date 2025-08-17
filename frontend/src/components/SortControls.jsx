import React from 'react';
import { SortDesc } from 'lucide-react';

const SortControls = ({ sortBy, onSortChange }) => {
  return (
    <div className="content-header">
      <div className="sort-controls">
        <SortDesc size={16} />
        <select value={sortBy} onChange={(e) => onSortChange(e.target.value)}>
          <option value="newest">Newest First</option>
          <option value="oldest">Oldest First</option>
          <option value="risk">Highest Risk</option>
          <option value="sentiment">Most Positive</option>
          <option value="confidence">Highest Confidence</option>
        </select>
      </div>
    </div>
  );
};

export default SortControls;
