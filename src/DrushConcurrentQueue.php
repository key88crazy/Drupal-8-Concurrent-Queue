<?php

namespace Drupal\concurrent_queue;

use Drupal\Core\Database\Database;
use Drupal\Core\Queue\QueueWorkerInterface;
use Drupal\Core\Queue\ReliableQueueInterface;
use Drupal\Core\Queue\RequeueException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\Exception;

/**
 * A wrapper for a queue, paired with its worker and other command line options.
 */
class DrushConcurrentQueue {

  /**
   * Options from the drush command invocation
   *
   * @var array
   */
  protected $options;

  /**
   * @var ReliableQueueInterface
   */
  protected $queue;

  /**
   * The queue worker plugin definition
   *
   * @var array
   */
  protected $info;

  /**
   * The name of the queue
   *
   * @var string
   */
  protected $name;

  /**
   * @var QueueWorkerInterface
   */
  protected $worker;

  /**
   * @var LoggerInterface
   */
  protected $logger;

  /**
   * @var int|null
   */
  protected $startTime;

  /**
   * @param $queue_name
   * @param ReliableQueueInterface $queue
   * @param QueueWorkerInterface $worker
   * @param array $queue_definitions
   * @param array $options
   * @param LoggerInterface $logger
   */
  public function __construct($queue_name, ReliableQueueInterface $queue, QueueWorkerInterface $worker, array $queue_definitions, array $options, LoggerInterface $logger) {
    $this->name = $queue_name;
    $this->queue = $queue;
    $this->worker = $worker;
    $this->info = $queue_definitions[$queue_name];
    $this->logger = $logger;
    $this->startTime = NULL;
  }

  public function __call($name, $arguments) {
    return call_user_func_array(array($this->queue, $name), $arguments);
  }

  /**
   * Reclaim items from the queue.
   *
   * @return bool|\Drupal\Core\Database\StatementInterface|int|null
   */
  public function reclaimItems() {
    if (method_exists($this->queue, 'reclaimItems')) {
      return $this->queue->reclaimItems();
    }
    elseif ($this->queue instanceof ReliableQueueInterface) {
      // If a parent queue is being run, reclaim all un-run and expired items
      // assigned to subqueues or the parent queue.
      $query = \Drupal::database()->update('queue');
      $query->fields([
        'expire' => 0,
        'name' => $this->name
      ]);

      $db_like = \Drupal::database()->escapeLike($this->name . ':');

      $expire_condition = $query->orConditionGroup()
        ->condition('expire', 0)
        ->condition('expire', time(), '<');

      $name_condition = $query->orConditionGroup()
        ->condition('name', $this->name)
        ->condition('name', $db_like . '%', 'LIKE');

      return $query->condition($expire_condition)->condition($name_condition)->execute();
    }
    else {
      $this->logger->warning(dt("Unable to reclaim items for @queue queue since it does not use or extend the DatabaseQueue class.", array('@queue' => $this->name)));
      return FALSE;
    }
  }

  /**
   * Get the timelimit if set on the queue info.
   *
   * @param bool $set
   * @return int|null
   */
  public function getTimeLimit($set = TRUE) {
    $max_execution_time = ini_get('max_execution_time');

    if (!isset($this->timeLimit)) {
      if (!$max_execution_time) {
        // If there is no limit to the max execution time, then set a more
        // reasonable limit of one day running time.
        $this->timeLimit = 86400;
      }
      else {
        $this->timeLimit = (isset($this->info['cron']['time']) ? $this->info['cron']['time'] : 60);
      }
    }

    // Try to increase the maximum execution time if it is too low.
    if ($set && $max_execution_time > 0 && $max_execution_time < ($this->timeLimit + 30) && !ini_get('safe_mode')) {
      $this->logger->info(dt("Calling set_time_limit(@value).", array('@value' => $this->timeLimit + 30)));
      @set_time_limit($this->timeLimit + 30);
    }

    return $this->timeLimit;
  }

  public function startTimer() {
    $this->startTime = microtime(TRUE);
  }

  public function readTimer() {
    return (microtime(TRUE) - $this->startTime);
  }

  /**
   * Spin off a specific number of concurrent workers to run queues.
   *
   * @param int $concurrency
   *   The number of worker queues to start.
   *
   * @return int
   *   The number of queue items processed by the worker queues.
   */
  public function invokeWorkers($concurrency) {
    $invocations = array_fill(0, $concurrency, array(
      'site' => '@self',
      'command' => 'queue-run',
      'args' => array($this->name),
    ));

    $commandline_options = array(
      'concurrency' => 1,
      'no-reclaim' => TRUE,
      // Internal flag to indicate this is a worker queue.
      'strict' => 0,
      'is-worker' => TRUE,
    );
    foreach (array('override-time-limit') as $option) {
      $value = $this->options[$option];
      if (isset($value)) {
        $commandline_options[$option] = $value;
      }
    }
    $backend_options = array(
      'concurrency' => count($invocations),
    );

    $queue_item_count = $this->queue->numberOfItems();

    $this->logger->info(dt("Starting @concurrency queue @name workers. There are @count items in the queue.", array(
      '@concurrency' => $concurrency,
      '@name' => $this->name,
      '@count' => $queue_item_count
    )));

    $this->getTimeLimit();

    $this->startTimer();
    $results = drush_backend_invoke_concurrent($invocations, $commandline_options, $backend_options);
    $elapsed = $this->readTimer();

    // Calculate the number of items actually processed using the results from
    // the drush_backend_invoke_concurrent() call.
    $count = 0;
    if (!empty($results['concurrent'])) {
      foreach ($results['concurrent'] as $result) {
        foreach ($result['log'] as $log) {
          if (preg_match('/Processed (\d+) items from the ' . preg_quote($this->name, '/') . ' queue/', $log['message'], $matches)) {
            $count += $matches[1];
          }
        }
      }
    }

    // Force the database connection to be reconnected since the workers may
    // have run for an extended amount of time.
    Database::closeConnection();
    $message = dt("Concurrently processed @count items from queue @queue in @elapsed sec (@rate/min). There are @remaining remaining items.",
      array(
        '@count' => $count,
        '@queue' => $this->name,
        '@elapsed' => round($elapsed, 2),
        '@rate' => round(($count / $elapsed) * 60),
        '@remaining' => $queue_item_count - $count
      )
    );

    if (drush_get_error()) {
      $this->logger->warning($message);
    }
    else {
      $this->logger->success($message);
    }

    return $count;
  }

  /**
   * Run a queue process for as long as possible.
   *
   * @return int
   *   The number of items successfully processed from the queue.
   */
  public function run() {
    $queue_worker = $this->worker;
    if ($limit = $this->options['override-time-limit']) {
      $this->timeLimit = $limit;
    }
    $limit = $this->getTimeLimit();
    $end = time() + $limit;
    $count = 0;

    $this->logger->info(dt("Running the queue @queue for a maximum of @time or until empty. There are @count items in the queue.", array(
      '@queue' => $this->name,
      '@time' => \Drupal::service('date.formatter')->formatInterval($limit),
      '@count' => $this->queue->numberOfItems(),
    )));

    $this->startTimer();
    while (time() < $end && $item = $this->queue->claimItem()) {
      try {
        $this->logger->info(dt("Processing item @id from @queue.", array('@id' => $item->item_id, '@queue' => $this->name)));
        $queue_worker->processItem($item->data);
        $this->queue->deleteItem($item);
        $count++;
      }
      catch (RequeueException $e) {
        $this->logger->error($e->getMessage());
        throw new Exception('Error with processing queue item.');
      }
    }
    $elapsed = $this->readTimer();

    $this->logger->log(drush_get_error() ? 'warning' : 'success',
      dt("Processed @count items from queue @queue in @elapsed sec (@rate/min). There are @remaining remaining items.",
        array(
          '@count' => $count,
          '@queue' => $this->name,
          '@elapsed' => round($elapsed, 2),
          '@rate' => round(($count / $elapsed) * 60),
          '@remaining' => $this->queue->numberOfItems(),
        )
      )
    );

    return $count;
  }
}
