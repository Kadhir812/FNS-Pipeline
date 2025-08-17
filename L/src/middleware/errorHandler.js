// Global error handler middleware
export const errorHandler = (err, req, res, next) => {
  console.error('Error:', err);

  // Default error
  let error = {
    success: false,
    message: 'Internal Server Error',
    timestamp: new Date().toISOString()
  };

  // Elasticsearch errors
  if (err.name === 'ResponseError') {
    error.message = 'Database query failed';
    error.statusCode = 500;
  }

  // Validation errors
  if (err.name === 'ValidationError') {
    error.message = 'Validation failed';
    error.statusCode = 400;
    error.details = err.details;
  }

  // JSON parsing errors
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    error.message = 'Invalid JSON format';
    error.statusCode = 400;
  }

  // Include error details in development
  if (process.env.NODE_ENV === 'development') {
    error.stack = err.stack;
    error.originalError = err.message;
  }

  const statusCode = error.statusCode || err.statusCode || 500;
  res.status(statusCode).json(error);
};

// 404 handler
export const notFoundHandler = (req, res) => {
  res.status(404).json({
    success: false,
    message: `Route ${req.originalUrl} not found`,
    timestamp: new Date().toISOString()
  });
};

export default { errorHandler, notFoundHandler };
