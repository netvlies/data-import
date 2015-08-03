<?php

namespace Ddeboer\DataImport\Writer;

use Ddeboer\DataImport\Writer;

/**
 * @author Markus Bachmann <markus.bachmann@bachi.biz>
 */
class BatchWriter implements Writer
{
    /**
     * @var Writer
     */
    protected $delegate;

    /**
     * @var int
     */
    protected $size;

    /**
     * @var \SplQueue
     */
    protected $queue;

    /**
     * @param Writer $delegate
     * @param int $size
     */
    public function __construct(Writer $delegate, $size = 20)
    {
        $this->delegate = $delegate;
        $this->size = $size;
    }

    public function prepare()
    {
        $this->delegate->prepare();

        $this->queue = new \SplQueue();
    }

    /**
     * @param array $item
     */
    public function writeItem(array $item)
    {
        $this->queue->enqueue($item);

        if (count($this->queue) >= $this->size) {
            $this->flush();
        }

    }

    public function finish()
    {
        $this->flush();

        $this->delegate->finish();
    }

    protected function flush()
    {
        while (!$this->queue->isEmpty()) {
            $this->delegate->writeItem($this->queue->dequeue());
        }

        if ($this->delegate instanceof FlushableWriter) {
            $this->delegate->flush();
        }
    }
}
