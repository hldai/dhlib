import time
import multiprocessing as mp
from typing import Iterator


class MPItem:
    def __init__(self, data_index, data):
        self.data_index = data_index
        self.data = data


class DataProcessMP:
    def __init__(self, n_processes, n_lines_per_run=50):
        self.n_processes = n_processes
        self.p2c_queue = mp.Queue()
        self.c2p_queue = mp.Queue()
        self.n_lines_per_run = n_lines_per_run

        self._processes = list()

    def produce_data(self, pidx) -> Iterator[MPItem]:
        raise NotImplementedError

    def consume_data(self, data_iter):
        raise NotImplementedError

    def _process(self, pidx):
        cur_run_line_cnt, processed_line_cnt = 0, 0
        is_first = True
        for i, record in enumerate(self.produce_data(pidx)):
            if is_first:
                print('process {} starting from line {}'.format(pidx, i))
                is_first = False

            # self.p2c_queue.put(('pred', {'id': i, 'data': data}))
            self.p2c_queue.put(('pred', record))
            processed_line_cnt += 1
            cur_run_line_cnt += 1
            if cur_run_line_cnt == self.n_lines_per_run:
                self.p2c_queue.put(('done', None))
                self.c2p_queue.get()
                cur_run_line_cnt = 0

        # f = open('d:/data/fet/ontonotes_uf/g_train.json', encoding='utf-8')
        # for i, line in enumerate(f):
        #     if i % self.n_processes != pidx:
        #         continue
        #     if is_first:
        #         print('process {} starting from line {}'.format(pidx, i))
        #         is_first = False
        #
        #     self.p2c_queue.put(('pred', {'id': i, 'data': line}))
        #     processed_line_cnt += 1
        #     cur_run_line_cnt += 1
        #     if cur_run_line_cnt == self.n_lines_per_run:
        #         self.p2c_queue.put(('done', None))
        #         self.c2p_queue.get()
        #         cur_run_line_cnt = 0
        # f.close()

        if cur_run_line_cnt > 0:
            self.p2c_queue.put(('done', None))
            self.c2p_queue.get()
        self.p2c_queue.put(('end', None))
        print('process {} exit, {} batches processed'.format(pidx, processed_line_cnt))

    def run(self):
        self._init_processes()

        # fout = open('d:/data/tmp/tmp_1.txt', 'w', encoding='utf-8')
        # for i, data in enumerate(self._data_from_queue()):
        #     for v in data:
        #         fout.write('{}'.format(v['data']))
        # fout.close()
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


class FileCopyMP(DataProcessMP):
    def __init__(self, data_file, n_processes, n_lines_per_run):
        super().__init__(n_processes, n_lines_per_run)
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
    mptask = FileCopyMP('d:/data/fet/ontonotes_uf/ontonotes_full.json', 4, n_lines_per_run=1000)
    tbeg = time.time()
    mptask.run()
    print(time.time() - tbeg)
