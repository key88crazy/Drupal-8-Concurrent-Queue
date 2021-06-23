<?php

namespace Drupal\concurrent_queue\Commands;

use Consolidation\AnnotatedCommand\CommandData;
use Consolidation\AnnotatedCommand\CommandError;
use Drupal\Core\Queue\QueueFactory;
use Drupal\Core\Queue\QueueWorkerManagerInterface;
use Drupal\Core\Queue\SuspendQueueException;
use Drush\Commands\DrushCommands;
use Drupal\concurrent_queue\DrushConcurrentQueue;

/**
 * A Drush commandfile.
 *
 * In addition to this file, you need a drush.services.yml
 * in root of your module, and a composer.json file that provides the name
 * of the services file to use.
 *
 * See these files for an example of injecting Drupal services:
 *   - http://cgit.drupalcode.org/devel/tree/src/Commands/DevelCommands.php
 *   - http://cgit.drupalcode.org/devel/tree/drush.services.yml
 */
class ConcurrentQueueCommands extends DrushCommands {

  /**
   * @var \Drupal\Core\Queue\QueueWorkerManagerInterface
   */
  protected $workerManager;

  /**
   * @var \Drupal\Core\Queue\QueueFactory
   */
  protected $queueService;

  /**
   * Keep track of queue definitions.
   *
   * @var array
   */
  protected static $queues;

  public function __construct(QueueWorkerManagerInterface $workerManager, QueueFactory $queueService) {
    $this->workerManager = $workerManager;
    $this->queueService = $queueService;
  }

  /**
   * Concurrently run a specific queue by name
   *
   * @param $queue_name
   *   The name of the queue to run, as defined in either hook_queue_info or hook_cron_queue_info.
   * @param array $options An associative array of options whose values come from cli, aliases, config, etc.
   * @return int
   *  The number of items successfully processed
   *
   * @option concurrency
   *   The number of parallel processes to run. The default concurrency is 2.
   * @option override-time-limit
   *   Override the time limit set in hook_cron_queue_info() when processing. Time in seconds.
   * @option no-reclaim
   *   Will not attempt to reclaim items from the queue before processing.
   * @usage drush queue-run-concurrent example_queue --concurrency=5 --override-time-limit=300
   *   Run the 'example_queue' queue with five different processes, each running for up to 5 minutes.
   *
   * @validate-queue queue_name
   * @command queue:run-concurrent
   * @aliases qrc,queue-run-concurrent
   */
  public function runConcurrent($queue_name, array $options = ['concurrency' => null, 'override-time-limit' => null, 'no-reclaim' => null]) {

    $worker = $this->workerManager->createInstance($queue_name);
    if (!$worker) {
      throw new SuspendQueueException(dt('Unable to create worker for the @queue queue.', array('@queue' => $queue_name)));
    }

    $queue = new DrushConcurrentQueue($queue_name, $this->getQueue($queue_name), $worker, $this->getQueues(), $options, $this->logger());

    // Reset expired items in process queues.
    if ($options['no-reclaim'] && $reclaim_count = $queue->reclaimItems()) {
      $this->logger->success(dt("Reset @count items that had expired in the @queue queue.", array('@count' => $reclaim_count, '@queue' => $queue_name)));
    }

    $count_before = $queue->numberOfItems();
    if (!$count_before) {
      $this->logger->info(dt("There are no items to process in the @name queue.", array('@name' => $queue_name)));
      return 0;
    }

    $concurrency = $options['concurrency'] != NULL ? (int) $options['concurrency'] : 2;
    if ($concurrency > 1) {
      return $queue->invokeWorkers($concurrency);
    }
    else {
      return $queue->run();
    }
  }

  /**
   * Validate that queue exists.
   *
   * Annotation value should be the name of the argument/option containing the name.
   *
   * @hook validate @validate-queue
   * @param \Consolidation\AnnotatedCommand\CommandData $commandData
   * @return \Consolidation\AnnotatedCommand\CommandError|null
   */
  public function validateQueueName(CommandData $commandData) {
    $arg_name = $commandData->annotationData()->get('validate-queue', null);
    $name = $commandData->input()->getArgument($arg_name);
    $all = array_keys(self::getQueues());
    if (!in_array($name, $all)) {
      $msg = dt('Queue not found: !name', ['!name' => $name]);
      return new CommandError($msg);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function getQueues() {
    if (!isset(static::$queues)) {
      static::$queues = [];
      foreach ($this->workerManager->getDefinitions() as $name => $info) {
        static::$queues[$name] = $info;
      }
    }
    return static::$queues;
  }

  /**
   * @return \Drupal\Core\Queue\QueueInterface
   */
  public function getQueue($queue_name)
  {
    return $this->queueService->get($queue_name);
  }

}
