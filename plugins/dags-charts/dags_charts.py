# encoding: utf-8

__author__ = "jet"
__version__ = "0.1"

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

class DagsRunningChart(BaseView):

    @expose("/")
    @login_required
    @provide_session
    def index(self,session=None):

        # if request.method == 'POST':
        #     request_data = request.get_json(force=True).get('uri')

        dagid = request.args.get('dagid')
        execution_date = request.args.get('execution_date')

        if (not execution_date) or len(execution_date) <0 :
            execution_date = date.today().isoformat()

        #today = date.today()
        #min_day = today - timedelta(days=-2)
        #max_day = today - timedelta(days=0)
        session = settings.Session()

        # this query only works on MySQL as it uses MySQL variables.
        # For postgres you should be able to use a window function which is much cleaner

        #session.execute('SET @row_number:=0')
        #session.execute("SET @dag_run:=''")
        dagidsql = ''
        if ( dagid) and  len(dagid)>0 and dagid.find(',')>=0 :
            dagidsql = ',' + dagid + ','
            mysql_query = """
                    SELECT
                      `id`,
                      `dag_id`,
                      `execution_date`,
                      `state`,
                      `run_id`,
                      `end_date`,
                      `start_date`
                    FROM
                      `dag_run` d
                        WHERE start_date >= :execution_date
                          AND start_date < DATE_ADD(:execution_date,INTERVAL 1 DAY )
                          AND :dagidsql like concat( '%,',dag_id,',%' )
                        order by dag_id asc,execution_date asc ,end_date asc
                            """

        elif ( dagid) and  len(dagid) >0 and dagid.find(',')<0 :
            dagidsql = dagid
            mysql_query = """
                    SELECT
                      `id`,
                      `dag_id`,
                      `execution_date`,
                      `state`,
                      `run_id`,
                      `end_date`,
                      `start_date`
                    FROM
                      `dag_run` d
                        WHERE start_date >= :execution_date
                          AND start_date < DATE_ADD(:execution_date,INTERVAL 1 DAY )
                          AND ( dag_id like concat( '%',:dagidsql,'%' ) ) 
                        order by dag_id asc,execution_date asc ,end_date asc
                            """
        else: 
            mysql_query = """
                    SELECT
                      `id`,
                      `dag_id`,
                      `execution_date`,
                      `state`,
                      `run_id`,
                      `end_date`,
                      `start_date`
                    FROM
                      `dag_run` d
                        WHERE start_date >= :execution_date
                          AND start_date < DATE_ADD(:execution_date,INTERVAL 1 DAY )
                        order by dag_id asc,execution_date asc ,end_date asc
                            """


        result = session.execute(mysql_query,
            {'execution_date': execution_date
              , 'dagidsql': dagidsql  # MySQLdb.escape_string(dagidsql) 
            }
        )
        #logging.warning('RESULT-FROM-QUERY:')

        if result.rowcount > 0:
            records = result.fetchall()
            # dag_runs = [
            #      {
            #         'dagId': run['dag_id'],
            #         'startDate': run['start_date'].isoformat('T') + 'Z',
            #         'endDate': run['end_date'].isoformat('T') + 'Z' if run['end_date'] else datetime.now().isoformat('T') + 'Z',
            #         'executionDate': run['execution_date'].isoformat() + '.000Z',
            #         'executionDateStr': run['execution_date'].strftime("%Y-%m-%d %H:%M:%S"),
            #         'id': run['id'],  # TODO: how can we get the scheduled interval
            #         'state': run['state']
            #     } for run in records]

            # gantt chart data
            tasks = []
            tasksUniq = set() # set 
            yAxisLeftOffset = 0
            for dagInst in records:  
                end_date = dagInst.end_date if dagInst.end_date else datetime.now() 
                tasksUniq.add(dagInst.dag_id)
                if yAxisLeftOffset < len(dagInst.dag_id) :
                    yAxisLeftOffset = len(dagInst.dag_id)
                tasks.append({
                    'startDate': wwwutils.epoch(dagInst.start_date),
                    'endDate': wwwutils.epoch(end_date),
                    'isoStart': dagInst.start_date.isoformat()[:-7],
                    'isoEnd': end_date.isoformat()[:-7],
                    'taskName': dagInst.dag_id,
                    'duration': "{}".format(end_date - dagInst.start_date)[:-7],
                    'status': dagInst.state,
                    'runId': dagInst.run_id,
                    'executionDate': dagInst.execution_date.isoformat(),
                })

            if len(tasksUniq) < 2 :
                height = len(tasksUniq) * 50 + 40
            elif len(tasksUniq) < 3 :
                height = len(tasksUniq) * 50 + 30
            elif len(tasksUniq) < 4 :
                height = len(tasksUniq) * 50 + 20
            else : 
                height = len(tasksUniq) * 50
            states = {dagInst.state:dagInst.state for dagInst in records}
            data = {
                'taskNames': [dagInst.dag_id for dagInst in records],
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

        return self.render("dags-charts/dagsrunningchart.html"
                            , data=json.dumps(data, indent=2)
                            , execution_date=execution_date
                            , dagid=dagid
                            , dag=''
                          )



dags_charts_view = DagsRunningChart(category="Browse", name="Dags Running Chart")



class DagRunningHistoryChart(BaseView):
    CONSTANT_KWS = {
        "TASK_TYPES": 11,
        #"DAG_CREATION_MANAGER_LINE_INTERPOLATE": dcmp_settings.DAG_CREATION_MANAGER_LINE_INTERPOLATE,
        #"DAG_CREATION_MANAGER_QUEUE_POOL": dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL,
        #"DAG_CREATION_MANAGER_CATEGORYS": dcmp_settings.DAG_CREATION_MANAGER_CATEGORYS,
        #"DAG_CREATION_MANAGER_TASK_CATEGORYS": dcmp_settings.DAG_CREATION_MANAGER_TASK_CATEGORYS,
    }

    @expose("/")
    @login_required
    @provide_session
    def index(self,session=None):

        # if request.method == 'POST':
        #     request_data = request.get_json(force=True).get('uri')

        dagid = request.args.get('dagid')
        execution_date = request.args.get('execution_date')
        num_days = request.args.get('num_days')
        #dagid = 'dag008'
        if (not dagid) or len(dagid) <0 :
            dagid =  ''
        if (not num_days) :
            num_days =  7
        else:
            num_days = int(num_days)
        if (not execution_date) or len(execution_date) <0 :
            execution_date = date.today().isoformat()

        #today = date.today()
        #min_day = today - timedelta(days=-2)
        #max_day = today - timedelta(days=0)
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
                  DATE_FORMAT(start_date, '%Y-%m-%d')  execution_dt ,
                  CAST( CONCAT('2018-01-01 ', DATE_FORMAT(start_date, '%H:%i:%s')) AS DATETIME ) as start_date_display , 
                  CAST( CONCAT('2018-01-01 ', DATE_FORMAT( IFNULL(end_date,NOW()) , '%H:%i:%s')) AS DATETIME ) as end_date_display
                FROM
                  `dag_run` 
                    WHERE start_date >= DATE_ADD(:execution_date,INTERVAL :start_days DAY )
                      AND start_date < DATE_ADD(:execution_date,INTERVAL :end_days DAY )
                      AND dag_id = :dagid 
                      ORDER BY execution_date asc ,end_date asc
                        """

        result = session.execute(mysql_query,
            {'execution_date': execution_date
              , 'start_days': -1 * ( num_days -1 )
              , 'end_days': 1  # MySQLdb.escape_string(dagidsql) 
              , 'dagid': dagid
            }
        )
        #logging.warning('RESULT-FROM-QUERY:')

        if result.rowcount > 0:
            records = result.fetchall()
            # gantt chart data
            tasks = []
            tasksUniq = set() # set 

            # 修改图形距离左侧的，重写gantt-chart-d3v2.js的yAxisLeftOffset function
            yAxisLeftOffset = 0
            for dagInst in records:  
                tasksUniq.add(dagInst.execution_dt)
                if yAxisLeftOffset < len(dagInst.execution_dt) :
                    yAxisLeftOffset = len(dagInst.execution_dt)
                tasks.append({
                    'startDate': wwwutils.epoch(dagInst.start_date_display),
                    'endDate': wwwutils.epoch(dagInst.end_date_display),
                    'isoStart': dagInst.start_date.isoformat()[:-7],
                    'isoEnd': dagInst.end_date.isoformat()[:-7],
                    'taskName': dagInst.execution_dt,
                    'duration': "{}".format(dagInst.end_date - dagInst.start_date)[:-7],
                    'status': dagInst.state,
                    'runId': dagInst.run_id,
                    'executionDate': dagInst.execution_date.isoformat(),
                })

            # 修改横条的总高度
            if len(tasksUniq) < 2 :
                height = len(tasksUniq) * 50 + 40
            elif len(tasksUniq) < 3 :
                height = len(tasksUniq) * 50 + 30
            elif len(tasksUniq) < 4 :
                height = len(tasksUniq) * 50 + 20
            else : 
                height = len(tasksUniq) * 50
            states = {dagInst.state:dagInst.state for dagInst in records}
            data = {
                'taskNames': [dagInst.execution_dt for dagInst in records],
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

        return self.render("dags-charts/dagrunninghischart.html"
                        #    , dag_runs=json.dumps(dag_runs)
                            , data=json.dumps(data, indent=2)
                          #  , execution_date=datetime.now().isoformat()
                          #  , form= form
                            , execution_date = execution_date
                            , num_days=num_days
                            , dagid=dagid
                            , dag=''
                          )

dag_running_his_charts_view = DagRunningHistoryChart(category="Browse", name="Dag Running His Chart")

dags_charts_bp = Blueprint(
    "dags_charts_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/dags-charts"
)

class DagsRunningChartPlugin(AirflowPlugin):
    name = "dags_running_chart"
    operators = []
    flask_blueprints = [dags_charts_bp]
    hooks = []
    executors = []
    admin_views = [dags_charts_view]
    menu_links = []

class DagRunningHistoryChartPlugin(AirflowPlugin):
    name = "dag_running_his_chart"
    operators = []
    flask_blueprints = [dags_charts_bp]
    hooks = []
    executors = []
    admin_views = [dag_running_his_charts_view]
    menu_links = []
