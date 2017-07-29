# -*- coding:utf-8 -*-

import sys
import os
import datetime
import MySQLdb
import logging

# logger config  =======================================
logger = logging.getLogger()
logging.FileHandler(filename='daily_exp_point_init.log')
file_handler = logging.FileHandler(filename='daily_exp_point_init.log')
stream_handler = logging.StreamHandler();

formatter = logging.Formatter(
    '%(asctime)s  %(levelname)-8s %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.setLevel(logging.DEBUG)
# logger config end   =======================================

"""
  初始化日常任务,以时间维度进行初始化
"""

start_date = datetime.datetime.strptime('2017-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
end_date = datetime.datetime.strptime('2017-07-21 00:00:00', '%Y-%m-%d %H:%M:%S')

sys_admin_id = 2778

task_start_app = 9 # 启动APP
task_login_web = 10 #
task_read_news = 11
task_read_doc = 12
task_download_doc = 13
task_comment_reply = 14
task_favorite = 15
task_share = 16
task_question = 17
task_answer = 18

dbConfig = { \
    'HOST': '172.16.20.213', \
    'PORT': 3306, \
    'PWD': 'devDb150530', \
    'CHARSET': 'utf8'
}

# member
conn_member = MySQLdb.connect(host=dbConfig['HOST'], db='member',
                              user='member', passwd=dbConfig['PWD'],
                              port=dbConfig['PORT'], charset=dbConfig['CHARSET'])

cursor_member = conn_member.cursor(cursorclass=MySQLdb.cursors.DictCursor)

# statistics
conn_stat = MySQLdb.connect(host=dbConfig['HOST'], db='statistics',
                            user='statistics', passwd=dbConfig['PWD'],
                            port=dbConfig['PORT'], charset=dbConfig['CHARSET'])

cursor_stat = conn_stat.cursor(cursorclass=MySQLdb.cursors.DictCursor)

# wms 评论相关
conn_wms = MySQLdb.connect(host=dbConfig['HOST'], db='wms',
                           user='wms', passwd=dbConfig['PWD'],
                           port=dbConfig['PORT'], charset=dbConfig['CHARSET'])

cursor_wms = conn_wms.cursor(cursorclass=MySQLdb.cursors.DictCursor)

# pgc 提问，回答问题
conn_pgc = MySQLdb.connect(host=dbConfig['HOST'], db='pgc',
                           user='pgc', passwd=dbConfig['PWD'],
                           port=dbConfig['PORT'], charset=dbConfig['CHARSET'])

cursor_pgc = conn_pgc.cursor(cursorclass=MySQLdb.cursors.DictCursor)

task_dict = {}


def init_task():
    """
     初始化 task_dict
    :return:
    """
    sql = "select * from t_exp_task "
    cursor_member.execute(sql)
    results = cursor_member.fetchall()
    for row in results:
        task = Task(row['id'], row['name'], row['point'], row['max_num'])
        task_dict[row['id']] = task
    pass


def compute_exp_point(mem_id, task_id, operate_time, description):
    """

    :param mem_id:
    :param task_id:
    :param operate_time:
    :param description:
    :return:
    """
    try:
        # check_task_completed : operate_time
        completed = check_task_completed(mem_id=mem_id, task_id=task_id, operate_time=operate_time)
        if not completed:
            logger.debug("==== start compute exp point; ===== task_id is %s mem_id is %s", task_id, mem_id)
           # description = description +" : " +task_dict[task_id].name
            insert_exp_point_detail(mem_id,
                                    task_id=task_id,
                                    point=task_dict[task_id].point,
                                    description=description,
                                    creator=sys_admin_id,
                                    operate_time=operate_time)
            insert_or_update_total_point(mem_id=mem_id, point=task_dict[task_id].point)
            conn_member.commit()
        else:
            logger.debug("==== this task has  completed ===== task_id is %s mem_id is %s", task_id, mem_id)
            pass
    except MySQLdb.Error as e:
        conn_member.rollback()
        logger.error(" =========roll back ================ task_id is %s mem_id is %s", task_id, mem_id)
        logger.error(str(e))
        pass
    return


def check_task_completed(mem_id, task_id, operate_time):
    """

    :param mem_id:
    :param task_id:
    :param operate_time:
    :return:
    """
    sql = "SELECT COUNT(1) num  FROM t_exp_point_detail t WHERE t.task_id = %s AND t.mem_id = %s " \
          "AND t.operate_time  >= %s AND t.operate_time < %s "
    truncate_time = operate_time.replace(hour=0, minute=0, second=0, microsecond=0)
    cursor_member.execute(sql, (task_id, mem_id, truncate_time, truncate_time + datetime.timedelta(days=1)))
    fetchone = cursor_member.fetchone()
    return fetchone['num'] >= task_dict[task_id].max_num


def insert_exp_point_detail(mem_id, task_id, point, description, creator, operate_time):
    sql = "INSERT INTO t_exp_point_detail(mem_id, task_id, point, description, creator, operate_time)\
      VALUES (%s,%s,%s,%s,%s,%s)"

    cursor_member.execute(sql, (mem_id, task_id, point, description, creator, operate_time))
    return


def insert_or_update_total_point(mem_id, point):
    sql = "INSERT INTO t_exp_point_total(mem_id, point,update_time) \
        VALUES (%s,%s,current_timestamp) \
        ON DUPLICATE KEY UPDATE point = point+%s,update_time = current_timestamp "

    cursor_member.execute(sql, (mem_id, point, point))
    return


def get_start_app(op_date):
    """
    启动APP
    :param op_date:
    :return:
    """
    sql = "SELECT mem_id ,type,create_time op_time,source FROM t_member_log t WHERE type= 2 " \
          "AND t.create_time >= %s AND t.create_time < %s " \
          "AND t.source in (1,2) ORDER BY t.mem_id "
    cursor_member.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_member.fetchall()
    return results


def get_login_web(op_date):
    """
    登录网站
    :param op_date:
    :return:
    """
    sql = "SELECT mem_id ,type,create_time op_time,source FROM t_member_log t WHERE type= 2 " \
          "AND t.create_time >= %s AND t.create_time < %s " \
          "AND t.source = 0 ORDER BY t.mem_id "
    cursor_member.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_member.fetchall()
    return results


def get_read_news(op_date):
    """
    阅读资讯
    :param op_date:
    :return:
    """
    sql = "SELECT mem_id,ref_id data_id, type,create_time op_time FROM t_click_count_detail t " \
          "WHERE  t.valid_flag = 1 AND t.type = 1 AND t.mem_id != 0 " \
          "AND t.create_time >= %s AND t.create_time < %s ORDER BY t.mem_id "
    cursor_stat.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_stat.fetchall()
    return results


def get_read_doc(op_date):
    """
    阅读资料
    :param op_date:
    :return:
    """
    sql = "SELECT mem_id,ref_id data_id, type,create_time op_time FROM t_click_count_detail t " \
          "WHERE  t.valid_flag = 1 AND t.type = 2 AND mem_id != 0 " \
          "AND t.create_time >= %s AND t.create_time < %s ORDER BY t.mem_id "
    cursor_stat.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_stat.fetchall()
    return results

def get_download_doc(op_date):
    """
    资料下载
    :param op_date:
    :return:
    """
    sql = " SELECT  mem_id,doc_id data_id,source,create_time op_time " \
          "FROM t_doc_download_detail t " \
          "WHERE  t.mem_id != 0 AND t.create_time >= %s AND t.create_time < %s ORDER BY t.mem_id "
    cursor_stat.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_stat.fetchall()
    return results

def get_comment_or_reply(op_date):
    """
    评论/回复评论
    :param op_date:
    :return:
    """
    sql = " SELECT id,creator mem_id,data_id,data_type,pid,check_time op_time  " \
          "FROM t_comment t " \
          "WHERE t.status = 0 AND  t.creator_type = 1  AND t.checked = 1 " \
          "AND  t.check_time >= %s AND t.check_time < %s  AND t.creator != 0 ORDER BY t.check_time,t.creator "
    cursor_wms.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_wms.fetchall()
    return results

def get_favorite(op_date):
    """
    收藏
    :param op_date:
    :return:
    """
    sql = " SELECT t.id,t.mem_id,t.create_time op_time,t.info_id data_id,t.type data_type " \
          "FROM t_mem_favorite t " \
          "WHERE  t.create_time >= %s AND t.create_time < %s  AND t.mem_id != 0  ORDER BY t.mem_id "
    cursor_member.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_member.fetchall()
    return results

def get_share(op_date):
    """
    分享
    :param op_date:
    :return:
    """
    sql = " SELECT t.id,t.member_id mem_id,t.data_id,t.data_type, t.channel_type ,t.create_time op_time " \
          "FROM t_share_log t " \
          "WHERE t.create_time >= %s  AND t.create_time < %s  AND t.member_id != 0  ORDER BY t.member_id "
    cursor_stat.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_stat.fetchall()
    return results

def get_question(op_date):
    """
    提交问题
    :param op_date:
    :return:
    """
    sql = " SELECT t.id,t.mem_id,t.audit_time op_time " \
          " FROM t_question t  " \
          " WHERE (t.process_flag = 1 OR t.process_flag >= 3 ) AND t.mem_id != 0 " \
          " AND t.audit_time >= %s AND t.audit_time <  %s  ORDER BY t.mem_id"
    cursor_pgc.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_pgc.fetchall()
    return results

def get_answer(op_date):
    """
    回答问题
    :param op_date:
    :return:
    """
    sql = " SELECT t.id,t.mem_id,t.que_id,t.audit_time op_time " \
          " FROM t_answer t  WHERE t.audit_flag = 1 " \
          " AND t.audit_time >=  %s  AND t.audit_time <   %s AND t.mem_id != 0  ORDER BY t.mem_id "
    cursor_pgc.execute(sql, (op_date, op_date + datetime.timedelta(days=1)))
    results = cursor_pgc.fetchall()
    return results


def get_mem_ids(start, limit):
    """
    :param start:
    :param limit:
    :return:
    """
    sql = "SELECT id mem_id FROM t_member WHERE status = 0  ORDER BY register_time ASC LIMIT %s,%s"
    cursor_member.execute(sql, (start, limit))
    results = cursor_member.fetchall()
    return results


def init():
    logger.info(" ================== init  start ======================= ")
    init_task()
    op_date = start_date  # 推进时间
    while (op_date <= end_date):
        start_app = get_start_app(op_date)
        login_web = get_login_web(op_date)
        read_news = get_read_news(op_date)
        read_doc = get_read_doc(op_date)
        download_doc = get_download_doc(op_date)
        comment_or_reply = get_comment_or_reply(op_date)
        favorite = get_favorite(op_date)
        share = get_share(op_date)
        question = get_question(op_date)
        answer = get_answer(op_date)

        description = "初始化日常任务"
        for e in start_app:
            logger.debug(" start_app : " + str(e))
            compute_exp_point(e['mem_id'], task_start_app, e['op_time'], description)
            pass
        for e in login_web:
            logger.debug("login_web : %s ", e)
            compute_exp_point(e['mem_id'], task_login_web, e['op_time'], description)
            pass

        for e in read_news:
            logger.debug("read_news : %s ", e)
            compute_exp_point(e['mem_id'], task_read_news, e['op_time'], description)
            pass
        for e in read_doc:
            logger.debug("read_doc : %s ", e)
            compute_exp_point(e['mem_id'], task_read_doc, e['op_time'], description)
            pass
        for e in download_doc:
            logger.debug("download_doc : %s  ", e)
            compute_exp_point(e['mem_id'], task_download_doc, e['op_time'], description)
            pass

        ### 评论/回复评论 批量审核 特殊处理 ：
        mem_count_dict ={}
        for e in comment_or_reply:
            mem_id = e['mem_id']
            if mem_id in mem_count_dict:
                mem_count_dict[mem_id] = mem_count_dict[mem_id] +1
            else:
                mem_count_dict[mem_id]= 0
        for e in comment_or_reply:
            op_time = e['op_time'] + datetime.timedelta(seconds=mem_count_dict[e['mem_id']])
            compute_exp_point(e['mem_id'], task_comment_reply, op_time, description)
            mem_count_dict[e['mem_id']] =  mem_count_dict[e['mem_id']] -1

        ### ===========  评论/回复评论 end

        for e in favorite:
            logger.debug("favorite : %s  ", e)
            compute_exp_point(e['mem_id'], task_favorite, e['op_time'], description)
            pass
        for e in share:
            logger.debug("share : %s  ", e)
            compute_exp_point(e['mem_id'], task_share, e['op_time'], description)
            pass
        for e in question:
            logger.debug("question : %s  ", e)
            compute_exp_point(e['mem_id'], task_question, e['op_time'], description)
            pass
        for e in answer:
            logger.debug("answer : %s  ", e)
            compute_exp_point(e['mem_id'], task_answer, e['op_time'], description)
            pass

        # 向前推进一天
        op_date = op_date + datetime.timedelta(days=1)

    # 释放连接
    conn_member.close()
    conn_stat.close()
    conn_wms.close()
    conn_pgc.close()
    logger.info(" ================== init  end  ======================= ")
    pass


class Task:
    def __init__(self, id, name, point, max_num):
        self.id = id
        self.name = name
        self.point = point
        self.max_num = max_num


if __name__ == '__main__':
    init()
    pass
