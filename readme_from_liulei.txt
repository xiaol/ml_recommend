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
6.数据库中的表：user_topics_cf

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
6.线上模型路径：/root/ossfs/topic_models
7.数据库表：主题信息： topic_model_v2
           新闻预测：news_topic_v2
           用户topic画像： user_topics_v2

四 kmeans聚类
1. 服务文件：news_api_ml/kmeans_service.py
2. 工程目录：news_api_ml/graphlab_kmeans
3. supervisor中的部署进程：
    kmeans_9977:更新模型并重启nid消费进程。 用于更新模型后启动，启动9977后不需要再启动9979
    kmeans_9978:消费click队列,  log文件：graphlab_kmeans/log/kmeans_click.log
    kmeans_9979:消费nid,        log文件：graphlab_kmeans/log/kmeans.log
    kmeans_9981:click行为进队列  log文件：graphlab_kmeans/log/kmeans_9981.log
    kmeans_9100(未在supervisor中部署,需要手动起服务):提供更新模型时所需的api，包括创建模型、预测nid、预测click
        创建新模型：http://120.55.88.11:9100/kmeans/createmodel
        测试新闻预测：http://120.55.88.11:9100/kmeans/predict_nids?nid=10490224&nid=10492723&nid=10492727&nid=10492717&nid=10492536&nid=10490304&nid=10415902&nid=9593812&nid=10494104&nid=10494365

    kmeans_9976(未在supervisor中部署)：最初为了健康、养生两个频道的数据提供的api,使用已有的最新模型对某个频道的新闻做预测, log: graphlab_kmeans/log/kmeans_chnl.log
4. log: 参见3
5.模型路径：/root/ossfs/kmeans_models
6.数据库： 新闻预测：news_kmeans
          用户兴趣：user_kmeans_cluster




五 sim_hash去重算法
1. 服务文件：simhash_service.py
2. 工程目录：news_api_ml/sim_hash,  类文件：util/simhash.py
3. supervisro中的部署进程：
    simhash_9969:直接采用simhash算法去重。直接去除差距在0-6位内的新闻, log:sim_hash/log/log.txt
    simhahs_9970:借助sentence hash检测到的有相同句子的新闻，删除7-12位差别的新闻。 要求标题重复度达到30%。  log:sim_hash/log/log_sen.txt
4. log: 参见3
5.数据库： 新闻simhash： news_simhash
          重复新闻：news_simhash_map


六. 专题自动生成算法  -- 借助sentence hash得到
1. 服务文件：multi_viewpoint_service.py
2. 工程目录：multi_viewpoint
3. supervisor中的部署进程：
    multi_vp_9963:每一天转移一下sentence hash数据
    multi_vp_9965:计算新闻的sentence hash, 将专题信息加入队列, 还会把含有相同句子的新闻加入另一个队列用于检测simhash值并去重（sim_hash的9970进程）. log: multi_viewpoint/log/log_9965.txt
    multi_vp_9964:消费专题队列生成专题, log:multi_viewpoint/log/log_subject.txt
4. 数据库：  专题系列数据库沿用已有的几个表；
            专题-topic表：subject_topic
            专题关键句： topic_sentence


七。 跳板机及hpc重启流程：
重启跳板机和HPC操作步骤

1 重启hpc和跳板机
2 跳板机/root/VPN/VPN-ECS下执行./run4centos7.sh
3 hpc在/root/VPN/VPN-HPC下执行./run.sh 10.117.195.196  （其中10.117.195.196是跳板机内网地址）
4 跳板机上执行 firewall-cmd --zone=external --add-forward-port=port=9000-9999:proto=tcp:toport=9000-9999:toaddr=10.172.64.2
5 最终，跳板机防火墙状态：
[root@iZ23horv3ssZ VPN-ECS]# firewall-cmd --list-all --zone=public
public (default, active)
  interfaces: vpn_vpn0
  sources:
  services: dhcpv6-client ssh
  ports: 80/tcp
  masquerade: no
  forward-ports:
  icmp-blocks:
  rich rules:

[root@iZ23horv3ssZ VPN-ECS]# firewall-cmd --list-all --zone=external
external (active)
  interfaces: eth0 eth1
  sources:
  services: ssh
  ports: 5555/tcp 80/tcp
  masquerade: yes
  forward-ports: port=9000-9999:proto=tcp:toport=9000-9999:toaddr=10.172.64.2
  icmp-blocks:
  rich rules:

6 hpc上的防火墙是关闭的
7 总结：正向代理是通过vpn完成的；   反向代理通过跳板机上的防火墙设置完成。 另外，只重启跳板机不需要执行3；只重启hpc不需要执行2
8 备份：跳板机VPN_ECS备份到/root/software, hpc的VPN-HPC备份到/root/software
9 redis-server重启redis;     supervisord重启supervisor

八。 将for-hpc这个bucket挂载到/root/ossfs目录下，AccessKeyId是QK8FahuiSCpzlWG8，AccessKeySecret是TGXhTCwUoEU4yNEGsfZSDvp0dNqw2p，

    oss endpoint是http://oss-cn-shanghai.aliyuncs.com

    1. echo for-hpc: QK8FahuiSCpzlWG8:TGXhTCwUoEU4yNEGsfZSDvp0dNqw2p> /etc/passwd-ossfs
    2， chmod 640 /etc/passwd-ossfs
    3 mkdir /root/ossfs
    4 ossfs for-hpc /tmp/ossfs -ourl=http://oss-cn-shanghai.aliyuncs.com





