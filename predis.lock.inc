<?php

/**
 * @file
 * Drupal core lock.inc implementation using Redis via Predis library.
 * 
 * FIXME: This implementation is not perfect, it could have potential race
 * conditions at some point, when dealing with the owner flag. Need to find
 * elegant but fast solutions.
 */

require_once dirname(__FILE__) . '/phpredis.client.inc';

function lock_initialize() {
  global $locks;

  $locks = array();
}

function _lock_id() {
  static $lock_id;

  if (!isset($lock_id)) {
    // Assign a unique id.
    $lock_id = uniqid(mt_rand(), TRUE);

    // We only register a shutdown function if a lock is used.
    drupal_register_shutdown_function('lock_release_all', $lock_id);
  }
  return $lock_id;
}

function lock_acquire($name, $timeout = 30.0) {
  global $locks;

  $client  = Redis_Client::getClient();
  $key     = 'lock:' . $name;
  $id      = _lock_id();

  // Insure that the timeout is at least 1 second, we cannot do otherwise with
  // Redis, this is a minor change to the function signature, but in real life
  // nobody will notice with so short duration.
  $timeout = ceil(max($timeout, 1));

  if (isset($locks[$name])) {

    // Create a new transaction, for atomicity.
    //$client->multi();

    // Global tells us we are the owner, but in real life it could have expired
    // and another process could have taken it, check that.
    if ($client->get('lock:' . $name . ':owner') == $id) {

      $client->multi();
      // See comment below, there is no guarantee that the lock does not
      // expire while we are actually sending the transaction. At least it
      // really narrow down the actual potential race condition range, but
      // do not remove it.
      $client->expire($key, $timeout);
      $client->setex($key . ':owner', $timeout, $id);
      $client->exec();

      return TRUE;
    }

    //$client->discard();

    unset($locks[$name]);
  }
  else {
    $client->watch($key);

    if (1 != $client->incr($key)) {
      $client->unwatch();
      return FALSE;
    }

    $client->multi();
    // The INCR command should reset the EXPIRE state, so we are now the
    // official owner. Set the owner flag and real EXPIRE delay.
    $client->expire($key, $timeout);
    $client->setex($key . ':owner', $timeout, $id);
    $client->exec();

    // If another client modified the $key value, transaction will be discarded
    // $result will be set to FALSE. This means atomicity have been broken and
    // the other client took the lock instead of us. The another condition is
    // the INCR result test. If we succeeded in incrementing the counter but
    // that counter was more than 0, then someone else already have the lock
    // case in which we cannot proceed.
    /*
    if (FALSE === $result || 1 != $result[0]) {
      return FALSE;
    }
     */

    // Register the lock and return.
    return ($locks[$name] = TRUE);
  }

  return FALSE;
}

function lock_may_be_available($name) {
  $client  = Redis_Client::getClient();
  $key     = 'lock:' . $name;
  $id      = _lock_id();

  list($value, $owner) = $client->mget(array($key, $key . ':owner'));

  return (FALSE !== $value || 0 == $value) && $id == $owner;
}

function lock_wait($name, $delay = 30) {
  // Pause the process for short periods between calling
  // lock_may_be_available(). This prevents hitting the database with constant
  // database queries while waiting, which could lead to performance issues.
  // However, if the wait period is too long, there is the potential for a
  // large number of processes to be blocked waiting for a lock, especially
  // if the item being rebuilt is commonly requested. To address both of these
  // concerns, begin waiting for 25ms, then add 25ms to the wait period each
  // time until it reaches 500ms. After this point polling will continue every
  // 500ms until $delay is reached.

  // $delay is passed in seconds, but we will be using usleep(), which takes
  // microseconds as a parameter. Multiply it by 1 million so that all
  // further numbers are equivalent.
  $delay = (int) $delay * 1000000;

  // Begin sleeping at 25ms.
  $sleep = 25000;
  while ($delay > 0) {
    // This function should only be called by a request that failed to get a
    // lock, so we sleep first to give the parallel request a chance to finish
    // and release the lock.
    usleep($sleep);
    // After each sleep, increase the value of $sleep until it reaches
    // 500ms, to reduce the potential for a lock stampede.
    $delay = $delay - $sleep;
    $sleep = min(500000, $sleep + 25000, $delay);
    if (lock_may_be_available($name)) {
      // No longer need to wait.
      return FALSE;
    }
  }
  // The caller must still wait longer to get the lock.
  return TRUE;
}

function lock_release($name) {
  global $locks;

  $client  = Redis_Client::getClient();
  $key     = 'lock:' . $name;
  $id      = _lock_id();

  unset($locks[$name]);

  // FIXME: Atomicity. Problem here is the check.
  if ($client->get($key . ':owner' == $id)) {
    $client->multi();
    $client->del(array($key, $key . ':owner'));

    // Commit the atomic transaction.
    $client->exec();
  }
  else {
    $client->discard();
  }
}

function lock_release_all($lock_id = NULL) {
  global $locks;

  if (empty($locks)) {
    return;
  }

  $client  = Redis_Client::getClient();

  // We can afford to deal with a slow algorithm here, this should not happen
  // on normal run because we should have removed manually all our locks.
  foreach ($locks as $name => $foo) {
    $key = 'lock:' . $name;

    // FIXME: Once again, this is not atomic, see lock_release() documentation.
    if ($client->get($key . ':owner' == $id)) {
      $client->del(array($key, $key . ':owner'));
    }
  }
}