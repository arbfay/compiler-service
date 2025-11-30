export class AppError extends Error {
  status: number;
  code: string;
  details?: unknown;

  constructor(status: number, code: string, message: string, details?: unknown) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export class ValidationError extends AppError {
  constructor(details: unknown) {
    super(422, 'validation_failed', 'Request body does not meet screener requirements', details);
  }
}

export class BadRequestError extends AppError {
  constructor(message = 'Invalid request', details?: unknown) {
    super(400, 'bad_request', message, details);
  }
}
