# encoding: utf-8

__author__ = "jet.cai"
__version__ = "0.2" # 201803

import logging
import json
import difflib
from functools import wraps
from collections import OrderedDict

import airflow
from airflow import settings
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from airflow.utils.db import provide_session
from flask import Blueprint, Markup, request, jsonify, flash
from flask_admin import BaseView, expose
from flask_admin.babel import gettext
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

from dcmp import settings as dcmp_settings
from dcmp.models import DcmpDag, DcmpDagConf
from dcmp.dag_converter import dag_converter
from dcmp.utils import LogStreamContext, search_conf_iter
from datetime import datetime, date, timedelta

from airflow.www import utils as wwwutils

import dateutil.parser
from airflow.www.forms import DateTimeForm
import MySQLdb

def login_required(func):
# when airflow loads plugins, login is still None.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return func_wrapper

class DagRunningHistoryChart(BaseView):
    @expose("/")
    @login_required
    @provide_session
    def index(self,session=None):

        # if request.method == 'POST':
        #     request_data = request.get_json(force=True).get('uri')

        # 参数
        # dagid, execution_date, num_days , display

        dagid = request.args.get('dagid')
        execution_date = request.args.get('execution_date')
        num_days = request.args.get('num_days')
        display = request.args.get('display')      # day,day+dag,dag+dag+scheduler
        sort = request.args.get('sort') 

        #dagid = 'dag008'
        if (not dagid) or len(dagid) <0 :
            dagid =  ''
        if (not num_days) :
            num_days =  1
        else:
            num_days = int(num_days)
        if (not execution_date) or len(execution_date) <0 :
            execution_date = date.today().isoformat()
        if (not display) or len(display) <0 :
                    display = 'day+dag'
        if (not sort) or len(sort) <0 :
                    sort = 'time'


        curr_user = airflow.login.current_user
        # 是否为超级用户，普通用户只能看到自己
        dag_filter_sql=""
        if wwwutils.get_filter_by_user():
             dag_filter_sql = " ( dag_id IN ( SELECT dag_name  FROM dcmp_dag  WHERE last_editor_user_id = %s ) ) " % curr_user.user.id
        else:
            dag_filter_sql = " (1=1) " 

        # dagid 搜索条件
        dagid_search_sql = ""
        if ( dagid) and  len(dagid)>0 and dagid.find(',')>=0 :
            dagid_search_sql = " ('," + dagid + ",'" + " like concat( '%,',dag_id,',%' ) )  "
        elif ( dagid) and  len(dagid) >0 and dagid.find(',')<0 :
            dagid_search_sql = " ( dag_id like concat( '%','"+ dagid + "','%' ) ) "
        else: 
            dagid_search_sql = " (1=1) "

        if sort == 'time' :
            sortsql = '  start_date asc ,execution_date asc ,dag_id asc'
        else : sortsql = ' dag_id asc,execution_date asc ,start_date asc '


        session = settings.Session()
        mysql_query = """
                SELECT
                  `id`,
                  `dag_id`,
                  `execution_date`,
                  `state`,
                  `run_id`,
                  IFNULL(end_date,NOW()) end_date,
                  `start_date`,
                  DATE_FORMAT(start_date, '%%Y-%%m-%%d')  execution_dt ,
                  CAST( CONCAT('2018-01-01 ', DATE_FORMAT(start_date, '%%H:%%i:%%s')) AS DATETIME ) as start_date_display , 
                  DATE_ADD( CAST( CONCAT('2018-01-01 ', DATE_FORMAT(start_date, '%%H:%%i:%%s')) AS DATETIME ) 
                          , INTERVAL TIMESTAMPDIFF( SECOND, start_date , IFNULL(end_date,NOW()))  SECOND) end_date_display  
                FROM
                  `dag_run` 
                    WHERE start_date >= DATE_ADD(:execution_date,INTERVAL :start_days DAY )
                      AND start_date < DATE_ADD(:execution_date,INTERVAL :end_days DAY )
                      AND %s
                      AND %s
                      ORDER BY %s
                        """ % (dagid_search_sql,dag_filter_sql,sortsql)

        result = session.execute(mysql_query,
            {'execution_date': execution_date
              , 'start_days': -1 * ( num_days -1 )
              , 'end_days': 1  # MySQLdb.escape_string(dagidsql) 
            }
        )
        #logging.warning('RESULT-FROM-QUERY:')

        if result.rowcount > 0:
            records = result.fetchall()
            # gantt chart data
            tasks = []
            taskNames = []
            tasksUniq = set() # set 

            # 修改图形距离左侧的，重写gantt-chart-d3v2.js的yAxisLeftOffset function

            if display=='day' :
                for dagInst in records:  
                    tasks.append({
                        'startDate': wwwutils.epoch(dagInst.start_date_display),
                        'endDate': wwwutils.epoch(dagInst.end_date_display),
                        'isoStart': dagInst.start_date.isoformat()[:-7],
                        'isoEnd': dagInst.end_date.isoformat()[:-7],
                        'taskName': dagInst.execution_dt,
                        'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                        'status': dagInst.state,
                        'runId': dagInst.run_id,
                        'runName': dagInst.dag_id,
                        'executionDate': dagInst.execution_date.isoformat(),
                    })
                taskNames = [dagInst.execution_dt for dagInst in records]
            elif display=='day+dag' :
                for dagInst in records:  
                    tasks.append({
                        'startDate': wwwutils.epoch(dagInst.start_date_display),
                        'endDate': wwwutils.epoch(dagInst.end_date_display),
                        'isoStart': dagInst.start_date.isoformat()[:-7],
                        'isoEnd': dagInst.end_date.isoformat()[:-7],
                        'taskName': dagInst.execution_dt + '[' + dagInst.dag_id +']',
                        'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                        'status': dagInst.state,
                        'runId': dagInst.run_id,
                        'runName': dagInst.dag_id,
                        'executionDate': dagInst.execution_date.isoformat(),
                    })
                taskNames = [dagInst.execution_dt + '[' + dagInst.dag_id +']' for dagInst in records]
            elif (display=='dag+scheduler(1day)' and num_days==1 )  :    # dag+dag+scheduler
                for dagInst in records:  
                    tasks.append({
                        'startDate': wwwutils.epoch(dagInst.start_date_display),
                        'endDate': wwwutils.epoch(dagInst.end_date_display),
                        'isoStart': dagInst.start_date.isoformat()[:-7],
                        'isoEnd': dagInst.end_date.isoformat()[:-7],
                        'taskName': dagInst.dag_id + '[' + dagInst.run_id +']',
                        'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                        'status': dagInst.state,
                        'runId': dagInst.run_id,
                        'runName': dagInst.dag_id,
                        'executionDate': dagInst.execution_date.isoformat(),
                    })
                taskNames = [ dagInst.dag_id + '[' + dagInst.run_id +']'  for dagInst in records]    


            yAxisLeftOffset = 0
            for ta in tasks:
                tasksUniq.add(ta['taskName'])
                if yAxisLeftOffset < len(ta['taskName']) :
                    yAxisLeftOffset = len(ta['taskName'])

            # 修改横条的总高度
            if len(tasksUniq) < 2 :
                height = len(tasksUniq) * 45 + 35
            elif len(tasksUniq) < 3 :
                height = len(tasksUniq) * 45 + 25
            elif len(tasksUniq) < 4 :
                height = len(tasksUniq) * 45 + 15
            else : 
                height = len(tasksUniq) * 45
            states = {dagInst.state:dagInst.state for dagInst in records}
            data = {
                'taskNames': taskNames,
                'tasks': tasks,
                'taskStatus': states,
                'yAxisLeftOffset': yAxisLeftOffset * 6 ,
                'height': height,
            }

        else:
            logging.warning('No records found')
            #dag_runs = []
            data = []

        #dttm = datetime.now().date()
        #form = DateTimeForm(data={'execution_date': dttm})



        #logging.warning('##########################1')
        #logging.warning(mysql_query)
        #logging.warning('##########################2')
        #logging.warning(tasksUniq)
        #logging.warning('##########################3')
        #logging.warning(data)
        #logging.warning('##########################4')
        #logging.warning(tasks)
        #logging.warning('##########################5')
        #logging.warning(records)
        #logging.warning('##########################6')

        return self.render("dags-charts/dagrunninghischart.html"
                        #    , dag_runs=json.dumps(dag_runs)
                            , data=json.dumps(data, indent=2)
                          #  , execution_date=datetime.now().isoformat()
                          #  , form= form
                            , execution_date = execution_date
                            , num_days=num_days
                            , dagid=dagid
                            , display=display
                            , sort=sort
                            , dag=''
                          )



class TaskRunningHistoryChart(BaseView):
    @expose("/")
    @login_required
    @provide_session
    def index(self,session=None):
        dagid = request.args.get('dagid')   ## 只能单个
        taskid = request.args.get('taskid') ## 可多个
        execution_date = request.args.get('execution_date')
        num_days = request.args.get('num_days')
        display = request.args.get('display')      # day ,day+dag,day+dag+task
        sort = request.args.get('sort') 

        if (not dagid) or len(dagid) <0 :
            dagid =  ''
        if (not taskid) or len(taskid) <0 :
            taskid =  ''
        if (not num_days) :
            num_days =  1
        else:
            num_days = int(num_days)
        if (not display) or len(display) <0 :
                    display = 'day+dag'
        if (not sort) or len(sort) <0 :
                    sort = 'time'
        if (not execution_date) or len(execution_date) <0 :
            execution_date = date.today().isoformat()


        dag_filter_sql=""
        curr_user = airflow.login.current_user
        if wwwutils.get_filter_by_user():
             dag_filter_sql = " ( dag_id IN ( SELECT dag_name  FROM dcmp_dag  WHERE last_editor_user_id = %s ) ) " % curr_user.user.id
        else:
            dag_filter_sql = " (1=1) " 

        # dagid 搜索条件
        dagid_search_sql = ""
        if ( dagid) and  len(dagid)>0 and dagid.find(',')>=0 :
            dagid_search_sql = " ('," + dagid + ",'" + " like concat( '%,',dag_id,',%' ) )  "
        elif ( dagid) and  len(dagid) >0 and dagid.find(',')<0 :
            dagid_search_sql = " ( dag_id like concat( '%','"+ dagid + "','%' ) ) "
        else: 
            dagid_search_sql = " (1=1) "

        # taskid 搜索条件
        taskid_search_sql = ""
        if ( taskid) and  len(taskid)>0 and taskid.find(',')>=0 :
            taskid_search_sql = " ('," + taskid + ",'" + " like concat( '%,',task_id,',%' ) )  "
        elif ( taskid) and  len(taskid) >0 and taskid.find(',')<0 :
            taskid_search_sql = " ( task_id like concat( '%','"+ taskid + "','%' ) ) "
        else: 
            taskid_search_sql = " (1=1) "


        if sort == 'time' :
            sortsql = '  start_date asc ,execution_date asc ,dag_id asc'
        else : sortsql = ' dag_id asc,execution_date asc ,start_date asc '

        session = settings.Session()
        mysql_query = """
                SELECT
                  `task_id`,
                  `dag_id`,
                  `execution_date`,
                  `state`,
                  DATE_FORMAT(execution_date, '%%Y-%%m-%%d %%H:%%i:%%s') as run_id,
                  IFNULL(end_date,NOW()) end_date,
                  `start_date`,
                  DATE_FORMAT(start_date, '%%Y-%%m-%%d')  execution_dt ,
                  CAST( CONCAT('2018-01-01 ', DATE_FORMAT(start_date, '%%H:%%i:%%s')) AS DATETIME ) as start_date_display , 
                  DATE_ADD( CAST( CONCAT('2018-01-01 ', DATE_FORMAT(start_date, '%%H:%%i:%%s')) AS DATETIME ) 
                          , INTERVAL TIMESTAMPDIFF( SECOND, start_date , IFNULL(end_date,NOW()))  SECOND) end_date_display  
                FROM
                  `task_instance` 
                    WHERE start_date >= DATE_ADD(:execution_date,INTERVAL :start_days DAY )
                      AND start_date < DATE_ADD(:execution_date,INTERVAL :end_days DAY )
                      and %s
                      and %s 
                      and %s
                      ORDER BY %s
                        """ % (taskid_search_sql , dag_filter_sql, dagid_search_sql, sortsql )

        result = session.execute(mysql_query,
            {'execution_date': execution_date
              , 'start_days': -1 * ( num_days -1 )
              , 'end_days': 1  # MySQLdb.escape_string(dagidsql) 
            }
        )



        if result.rowcount > 0:
            records = result.fetchall()
            # gantt chart data
            tasks = []
            taskNames = []
            tasksUniq = set() # set 


            if display=='day' :
                for dagInst in records:  
                    tasks.append({
                        'startDate': wwwutils.epoch(dagInst.start_date_display),
                        'endDate': wwwutils.epoch(dagInst.end_date_display),
                        'isoStart': dagInst.start_date.isoformat()[:-7],
                        'isoEnd': dagInst.end_date.isoformat()[:-7],
                        'taskName': dagInst.execution_dt,
                        'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                        'status': dagInst.state,
                        'runId': dagInst.run_id,
                        'runName': dagInst.dag_id+'.'+dagInst.task_id,
                        'executionDate': dagInst.execution_date.isoformat(),
                    })
                taskNames = [dagInst.execution_dt for dagInst in records]
            elif display=='day+dag' :
                for dagInst in records:  
                    tasks.append({
                        'startDate': wwwutils.epoch(dagInst.start_date_display),
                        'endDate': wwwutils.epoch(dagInst.end_date_display),
                        'isoStart': dagInst.start_date.isoformat()[:-7],
                        'isoEnd': dagInst.end_date.isoformat()[:-7],
                        'taskName': dagInst.execution_dt + '[' + dagInst.dag_id +']',
                        'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                        'status': dagInst.state,
                        'runId': dagInst.run_id,
                        'runName': dagInst.dag_id+'.'+dagInst.task_id,
                        'executionDate': dagInst.execution_date.isoformat(),
                    })
                taskNames = [dagInst.execution_dt + '[' + dagInst.dag_id +']' for dagInst in records]
            elif display=='day+dag+task' :
                for dagInst in records:  
                    tasks.append({
                        'startDate': wwwutils.epoch(dagInst.start_date_display),
                        'endDate': wwwutils.epoch(dagInst.end_date_display),
                        'isoStart': dagInst.start_date.isoformat()[:-7],
                        'isoEnd': dagInst.end_date.isoformat()[:-7],
                        'taskName': dagInst.execution_dt + '[' + dagInst.dag_id +'.'+dagInst.task_id +']',
                        'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                        'status': dagInst.state,
                        'runId': dagInst.run_id,
                        'runName': dagInst.dag_id+'.'+dagInst.task_id,
                        'executionDate': dagInst.execution_date.isoformat(),
                    })
                taskNames = [dagInst.execution_dt + '[' + dagInst.dag_id +'.'+dagInst.task_id +']' for dagInst in records]

            yAxisLeftOffset = 0
            for ta in tasks:
                tasksUniq.add(ta['taskName'])
                if yAxisLeftOffset < len(ta['taskName']) :
                    yAxisLeftOffset = len(ta['taskName'])
            # 修改横条的总高度
            if len(tasksUniq) < 2 :
                height = len(tasksUniq) * 45 + 35
            elif len(tasksUniq) < 3 :
                height = len(tasksUniq) * 45 + 25
            elif len(tasksUniq) < 4 :
                height = len(tasksUniq) * 45 + 15
            else : 
                height = len(tasksUniq) * 45
            states = {dagInst.state:dagInst.state for dagInst in records}
            data = {
                'taskNames': taskNames,
                'tasks': tasks,
                'taskStatus': states,
                'yAxisLeftOffset': yAxisLeftOffset * 6 ,
                'height': height,
            }
        else:
            logging.warning('No records found')
            #dag_runs = []
            data = []



        #logging.warning('##########################1')
        #logging.warning(mysql_query)
        #logging.warning('##########################2')
        #logging.warning(tasksUniq)
        #logging.warning('##########################3')
        #logging.warning(data)
        #logging.warning('##########################4')
        #logging.warning(tasks)
        #logging.warning('##########################5')
        #logging.warning(records)
        #logging.warning('##########################6')
        dttm = datetime.now().date()
        form = DateTimeForm(data={'execution_date': dttm})

        return self.render("dags-charts/taskrunninghischart.html"
                        #    , dag_runs=json.dumps(dag_runs)
                            , data=json.dumps(data, indent=2)
                          #  , execution_date=datetime.now().isoformat()
                          #  , form= form
                            , execution_date = execution_date
                            , num_days=num_days
                            , dagid=dagid
                            , taskid=taskid
                            , display=display
                            , sort=sort
                            , dag=''
                          )

dag_running_his_charts_view = DagRunningHistoryChart(category="Browse", name="Dag Runs His Chart")
task_running_his_charts_view = TaskRunningHistoryChart(category="Browse", name="Task Runs His Chart")



dags_charts_bp = Blueprint(
    "dags_charts_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/dags-charts"
)



class DagRunningHistoryChartPlugin(AirflowPlugin):
    name = "dag_runs_his_chart"
    operators = []
    flask_blueprints = [dags_charts_bp]
    hooks = []
    executors = []
    admin_views = [dag_running_his_charts_view]
    menu_links = []

class TaskRunningHistoryChartPlugin(AirflowPlugin):
    name = "task_runs_his_chart"
    operators = []
    flask_blueprints = [dags_charts_bp]
    hooks = []
    executors = []
    admin_views = [task_running_his_charts_view]
    menu_links = []
