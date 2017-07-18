#介绍本人完成的主要工作
一 base_service
1.服务文件： news_api_ml/base_service.py
  涉及文件：news_api_ml/redis_process/nid_queue.py
2.服务功能：爬虫后台在新闻入库后，调用base_service.py中的http服务：/news_queue/produce_nid, 将新闻nid
           分发到多个队列，供其他算法消费。
3.supervisor 中的部署进程：base_service_9000
4. log: news_api_ml/base_service/log/log.txt

二。 user-based cf算法
1.服务文件: news_api_ml/cf_service.py
2.工程目录:news_api_ml/user_based_cf
3.supervisor中的部署进程：
    cf_9948:清理数据库中数据的进程
    cf_9949: cf算法计算进程
4.流程说明：
    入口函数: user_based_cf/data_process.py --> get_user_topic_cf()
    流程中步骤参考文件中注释
5.log: news_api_ml/user_based_cf/log/log.txt

三 主题模型
1. 服务文件：news_api_ml/lda_service.py
2. 工程目录：news_api_ml/graphlab_lda
3. supervisor中的部署进程：
    lda_9988:消费新闻nid队列
    lda_9989:用户点击事件入队列
    lda_9990:消费用户点击事件
    lda_9985:更新模型时所需http接口
4.流程说明

5.log:news_api_mo/graphlab_lda/log/log_9988_3.log、log_9989_3.log、log_9990_3.log

四 kmeans聚类
1. 服务文件：news_api_ml/kmeans_service.py
2. 工程目录：news_api_ml/graphlab_kmeans
3. supervisor中的部署进程：
    kmeans_9977:更新模型并重启nid消费进程。 用于更新模型后启动，启动9977后不需要再启动9979
    kmeans_9978:消费click队列
    kmeans_9979:消费nid
    kmeans_9981:click行为进队列
    kmeans_9100(未在supervisor中部署,需要手动起服务):提供更新模型时所需的api，包括创建模型、预测nid、预测click
        创建新模型：http://120.55.88.11:9100/kmeans/createmodel
        测试新闻预测：http://120.55.88.11:9100/kmeans/predict_nids?nid=10490224&nid=10492723&nid=10492727&nid=10492717&nid=10492536&nid=10490304&nid=10415902&nid=9593812&nid=10494104&nid=10494365

    kmeans_9976(未在supervisor中部署)：最初为了健康、养生两个频道的数据提供的api,使用已有的最新模型对某个频道的新闻做预测


五 sim_hash去重算法
1. 服务文件：simhash_service.py
2. 工程目录：news_api_ml/sim_hash,  类文件：util/simhash.py
3. supervisro中的部署进程：
    simhash_9969:直接采用simhash算法去重。直接去除差距在0-6位内的新闻
    simhahs_9970:借助句子hash检测到的有相同句子的新闻，删除7-12位差别的新闻。 要求标题




