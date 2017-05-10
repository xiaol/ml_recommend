
关于去除广告的接口说明:
1. git分支路径:
    https://github.com/xiaol/news_api_ml

2. 接口说明
   （1） 对外提供的去除广告的接口:
         http://120.55.88.11/news_process/RemoveAdsOnnid?nid=8186701
         其中8186701是新闻的nid。
         本服务由nginx转发,目前提供两个进程的并发;
         本接口会将nid放至Redis队列
   （2）nid队列的消费进程
       不对外提供。占用9998端口。 阻塞式从redis队列中取nid,调用去除广告的内部接口
   （3）人工干预广告及去除广告的服务
       http://120.55.88.11:9999/static/index.html  用于查看检测的广告信息及一系列干预处理
       http://120.55.88.11:9999/news_process/NewsAdsExtract 用于广告检测。 新增公众号后需要调用该接口
       http://120.55.88.11:9999/static/index.html  用于查看、处理新增和修改了的广告规则

接口说明:
    url: http://120.55.88.11:9000/news_queue/produce_nid
    参数: nid -----新闻id, int类型
    方法类型:GET方法
    用途说明:用于新闻入库后的处理,目前提供广告去除和主题模型服务。 服务会将nid放入redis队列,不会阻塞

