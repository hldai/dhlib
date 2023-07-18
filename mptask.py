import time
import multiprocessing as mp
from typing import Iterator


class MPItem:
    def __init__(self, data_index, data):
        self.data_index = data_index
        self.data = data


class SingleSrcDataProcessMP:
    def __init__(self, n_processes, n_items_per_run=50):
        self.n_processes = n_processes
        self.p2c_queue = mp.Queue()
        self.c2p_queues = [mp.Queue() for _ in range(n_processes)]
        self.n_items_per_run = n_items_per_run

        self._processes = list()

    def source_data_iter(self):
        raise NotImplementedError

    def process_data(self, data: MPItem) -> MPItem:
        raise NotImplementedError

    def consume_data(self, data_iter):
        raise NotImplementedError

    def _process(self, pidx):
        processed_cnt = 0
        while True:
            msg, data = self.c2p_queues[pidx].get()
            if msg == 'data':
                result = self.process_data(data)
                processed_cnt += 1
                self.p2c_queue.put(('r', result))
            elif msg == 'done':
                self.p2c_queue.put(('done', None))
            else:
                break

        print('process {} exit, {} items processed'.format(pidx, processed_cnt))

    def run(self):
        self._init_processes()

        self.consume_data(self._processed_data_iter())

        self._wait_processes()

    def _src_data_distributed(self):
        for j in range(self.n_processes):
            self.c2p_queues[j].put(('done', None))

        done_cnt = 0
        all_results = list()
        while done_cnt < self.n_processes:
            msg, result = self.p2c_queue.get()
            if msg == 'done':
                done_cnt += 1
            else:
                all_results.append(result)

        all_results.sort(key=lambda x: x.data_index)
        return all_results

    def _processed_data_iter(self):
        data_cnt = 0
        for i, src_data in enumerate(self.source_data_iter()):
            pidx = i % self.n_processes
            self.c2p_queues[pidx].put(('data', src_data))
            data_cnt += 1
            if data_cnt >= self.n_items_per_run * self.n_processes:
                yield from self._src_data_distributed()
                data_cnt = 0

        if data_cnt > 0:
            yield from self._src_data_distributed()

        for i in range(self.n_processes):
            self.c2p_queues[i].put(('end', None))

    def _init_processes(self):
        processes = list()
        for i in range(self.n_processes):
            process = mp.Process(target=self._process, args=(i,))
            process.start()
            processes.append(process)

    def _wait_processes(self):
        print('main waiting ...')
        for p in self._processes:
            p.join()
        print('main end')


class DataProcessMP:
    def __init__(self, n_processes, n_items_per_run=50):
        self.n_processes = n_processes
        self.p2c_queue = mp.Queue()
        self.c2p_queue = mp.Queue()
        self.n_items_per_run = n_items_per_run

        self._processes = list()

    def produce_data(self, pidx) -> Iterator[MPItem]:
        raise NotImplementedError

    def consume_data(self, data_iter):
        raise NotImplementedError

    def _process(self, pidx):
        cur_run_line_cnt, processed_line_cnt = 0, 0
        for i, item in enumerate(self.produce_data(pidx)):
            if i == 0:
                print('process {} start'.format(pidx))

            # self.p2c_queue.put(('pred', {'id': i, 'data': data}))
            self.p2c_queue.put(('pred', item))
            processed_line_cnt += 1
            cur_run_line_cnt += 1
            if cur_run_line_cnt == self.n_items_per_run:
                self.p2c_queue.put(('done', None))
                self.c2p_queue.get()
                cur_run_line_cnt = 0

        if cur_run_line_cnt > 0:
            self.p2c_queue.put(('done', None))
            self.c2p_queue.get()
        self.p2c_queue.put(('end', None))
        print('process {} exit, {} batches processed'.format(pidx, processed_line_cnt))

    def run(self):
        self._init_processes()

        self.consume_data(self._data_from_queue())

        self._wait_processes()

    def _data_from_queue(self):
        end_cnt, done_cnt = 0, 0
        all_pred_results = list()
        while True:
            # print('waiting next value')
            msg, pred_result = self.p2c_queue.get()
            if msg == 'end':
                end_cnt += 1
            elif msg == 'done':
                done_cnt += 1
            else:
                all_pred_results.append(pred_result)

            if end_cnt + done_cnt == self.n_processes and all_pred_results:
                # all_pred_results.sort(key=lambda x: x['id'])
                all_pred_results.sort(key=lambda x: x.data_index)
                yield from all_pred_results

                for _ in range(self.n_processes):
                    self.c2p_queue.put('OK')
                all_pred_results = list()
                done_cnt = 0

            if end_cnt == self.n_processes:
                break

    def _init_processes(self):
        processes = list()
        for i in range(self.n_processes):
            process = mp.Process(target=self._process, args=(i,))
            process.start()
            processes.append(process)

    def _wait_processes(self):
        print('main waiting ...')
        for p in self._processes:
            p.join()
        print('main end')


class SSFileCopyMP(SingleSrcDataProcessMP):
    def __init__(self, data_file, n_processes, n_items_per_run):
        super().__init__(n_processes, n_items_per_run)
        self.data_file = data_file

    def source_data_iter(self):
        f = open(self.data_file, encoding='utf-8')
        for i, line in enumerate(f):
            yield MPItem(i, line)
        f.close()

    def process_data(self, data: MPItem) -> MPItem:
        return data

    def consume_data(self, data_iter):
        fout = open('d:/data/tmp/tmp_2.txt', 'w', encoding='utf-8', newline='\n')
        for i, item in enumerate(data_iter):
            fout.write(item.data)
            if (i + 1) % 1000 == 0:
                print(i + 1)
        fout.close()


class FileCopyMP(DataProcessMP):
    def __init__(self, data_file, n_processes, n_items_per_run):
        super().__init__(n_processes, n_items_per_run)
        self.data_file = data_file

    def produce_data(self, pidx):
        f = open(self.data_file, encoding='utf-8')
        for i, line in enumerate(f):
            if i % self.n_processes != pidx:
                continue
            # yield line
            yield MPItem(i, line)
        f.close()

    def consume_data(self, item_iter):
        fout = open('d:/data/tmp/tmp_1.txt', 'w', encoding='utf-8', newline='\n')
        for item in item_iter:
            # print(item.data_index, item.data)
            fout.write(item.data)
        fout.close()


if __name__ == '__main__':
    # mptask = DataProcessMP(4, n_lines_per_run=3)
    # mptask = FileCopyMP('d:/data/tmp/tmp_0.txt', 4, n_lines_per_run=3)
    # mptask = FileCopyMP('d:/data/fet/ontonotes_uf/ontonotes_full.json', 4, n_items_per_run=1000)
    # mptask = SSFileCopyMP('d:/data/tmp/tmp_0.txt', 4, n_items_per_run=3)
    mptask = SSFileCopyMP('d:/data/fet/ontonotes_uf/ontonotes_full.json', 4, n_items_per_run=1000)
    tbeg = time.time()
    mptask.run()
    print(time.time() - tbeg)
