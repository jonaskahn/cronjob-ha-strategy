class QueuePriorityConstant {
  static QUEUE_PRIORITY_LEVEL_HIGH = 3;
  static QUEUE_PRIORITY_LEVEL_MEDIUM = 2;
  static QUEUE_PRIORITY_LEVEL_LOW = 1;
  static QUEUE_PRIORITY_NAME_HIGH = 'high';
  static QUEUE_PRIORITY_NAME_MEDIUM = 'medium';
  static QUEUE_PRIORITY_NAME_LOW = 'low';

  constructor() {
    throw new Error('Should not initialize the queue priority');
  }
}

export { QueuePriorityConstant };
