import json
import time
import queue
import threading
import traceback
import app.service.FEService as FEService
import app.service.PreprocessService as preprocessService
import app.service.ExplorationService as ExplorationService
import app.service.ml.PredictService as PredictService
import app.service.ml.SecondClassification as SecondClassification
import app.dao.OperatorDao as OperatorDao


def model_thread_execute(spark_session, start_nodes):
    """
    多线程执行 model（执行流程）
    :param spark_session：
    :param start_nodes:['1','2'] model（执行流程启动的节点）
    :return:
    """

    class MyThread(threading.Thread):
        def __init__(self, threadID, name, q):
            threading.Thread.__init__(self)
            self.threadID = threadID
            self.name = name
            self.q = q

        def run(self):
            print("开启线程：" + self.name)
            process_data(self.name, self.q)
            print("退出线程：" + self.name)

    def process_data(threadName, q):
        print('-------进入线程：', threadName)

        while G.noExecFlag or G.execCounter or not workQueue.empty():
            print('-------进入线程内部循环：', not workQueue.empty())
            # TODO 多线程安全
            G.noExecFlag = 0
            G.execCounter += 1
            queueLock.acquire()
            if not workQueue.empty():
                operator_id = q.get()
                queueLock.release()
                # TODO:处理函数
                could_execute_operator_ids = operator_execute(spark_session, operator_id)
                if could_execute_operator_ids is False:
                    print("%s processing %s error" % (threadName, operator_id))
                else:
                    for item_id in could_execute_operator_ids:
                        if item_id != '' and item_id is not None:
                            q.put(item_id)
                    print("%s processing %s add %s to queue" % (
                        threadName, operator_id, ','.join(could_execute_operator_ids)))
                    print("q.size: %s --- workQueue.size: %s" % (q.qsize(), workQueue.qsize()))
            else:
                queueLock.release()
            G.execCounter -= 1
            time.sleep(1)

    class G:
        # 未开始执行
        noExecFlag = 1
        # 正在执行operator的个数
        execCounter = 0

    threadList = ["Thread-1", "Thread-2", "Thread-3"]
    queueLock = threading.Lock()
    workQueue = queue.Queue(10)
    threads = []
    threadID = 1

    # 填充队列
    queueLock.acquire()
    for word in start_nodes:
        workQueue.put(word)
    queueLock.release()

    # 创建新线程
    for tName in threadList:
        thread = MyThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # 等待队列清空
    while (not workQueue.empty()) or G.execCounter:
        pass

    # 等待所有线程完成
    for t in threads:
        t.join()
    print("退出主线程")


def operator_execute(spark_session, operator_id):
    """
    执行算子
    :param spark_session:
    :param operator_id:
    :return:
    """
    try:
        print("------执行算子------", "operator_id：", operator_id)
        # 查算子
        operator = OperatorDao.get_operator_by_id(operator_id)
        # 获取input_url
        config = json.loads(operator.operator_config)
        file_url_list = config['fileUrl']
        # 获取输入地址
        url_arr = []
        for file_url_dict in file_url_list:
            key = ''
            for ikey in file_url_dict.keys():
                key = ikey
            if operator_id == key:
                url_arr.append(file_url_dict[key])
            else:
                father = OperatorDao.get_operator_by_id(key)
                # 检查父节点是否准备就绪
                if father.status != 'success':
                    return []
                # TODO:暂定从0 开始
                father_output_url_index = file_url_dict[key]
                father_url_arr = father.operator_output_url.split('*,')
                url_arr.append(father_url_arr[father_output_url_index])
        # 算子函数
        if operator.operator_type_id == 1001:
            preprocessService.filter_multi_conditions(spark_session, operator_id, url_arr[0],
                                                      json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1002:
            preprocessService.sort(spark_session, operator_id, url_arr[0],
                                   json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1003:
            preprocessService.column_split(spark_session, operator_id, url_arr[0],
                                           json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1005:
            preprocessService.columns_merge(spark_session, operator_id, url_arr[0],
                                            json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1006:
            preprocessService.replace(spark_session, operator_id, url_arr[0],
                                      json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1007:
            preprocessService.fill_null_value(spark_session, operator_id, url_arr[0],
                                              json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1008:
            preprocessService.column_map(spark_session, operator_id, url_arr[0],
                                         json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 1009:
            preprocessService.random_split(spark_session, operator_id, url_arr[0],
                                           json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2001:
            FEService.quantile_discretization(spark_session, operator_id, url_arr[0],
                                              json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2002:
            FEService.vector_indexer(spark_session, operator_id, url_arr[0],
                                     json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2003:
            FEService.standard_scaler(spark_session, operator_id, url_arr[0],
                                      json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2004:
            FEService.pca(spark_session, operator_id, url_arr[0],
                          json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2005:
            FEService.string_indexer(spark_session, operator_id, url_arr[0],
                                     json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2006:
            FEService.one_hot_encoder(spark_session, operator_id, url_arr[0],
                                      json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2007:
            FEService.polynomial_expansion(spark_session, operator_id, url_arr[0],
                                           json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 2008:
            FEService.chiSqSelector(spark_session, operator_id, url_arr[0],
                                    json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 3001:
            ExplorationService.full_table_statistics(spark_session, operator_id, url_arr[0],
                                                     json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 3002:
            ExplorationService.frequency_statistics(spark_session, operator_id, url_arr[0],
                                                    json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 3003:
            ExplorationService.correlation_coefficient(spark_session, operator_id, url_arr[0],
                                                       json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 5001:
            preprocessService.read_data_with_update_record(spark_session, operator_id, url_arr[0])
        elif operator.operator_type_id == 6000:
            PredictService.ml_predict(spark_session, operator_id, url_arr,
                                      json.loads(operator.operator_config)['parameter'])
        elif operator.operator_type_id == 6001:
            SecondClassification.svm(spark_session, operator_id, url_arr[0],
                                     json.loads(operator.operator_config)['parameter'])

        return operator.child_operator_ids.split(',')

    except:
        traceback.print_exc()
        return False
