from jira import JIRA
from datetime import datetime,timedelta
import psycopg2.extras
import psycopg2
import json
import requests

def db_connect():
    try:
        conn_string = "host='172.0.0.1' dbname='jira' user='jiraUser' password='password'"
        return psycopg2.connect(conn_string)

    except Exception as e:
        print("database error " + str(e))

def hasDate(created):
    co = db_connect()
    cu = co.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cu.execute('SELECT * FROM other_jira."public".created_vs_solved WHERE date = %s;', [created])

    if bool(cu.fetchone()):
        result = True
    else:
        result = False

    co.commit()
    cu.close()
    co.close()
    return result

def get_row(created):
    connct = db_connect()
    curs = connct.cursor(cursor_factory=psycopg2.extras.DictCursor)
    curs.execute('SELECT * FROM other_jira."public".created_vs_solved WHERE date = %s;', [created])
    row = curs.fetchone()
    connct.commit()
    curs.close()
    connct.close()
    return row

def connectToJira():
    options = {
        'server': 'https://jira.org'}
    try:
        return JIRA(options, basic_auth=('user', 'Password'))
    except Exception as e:
        print('Connection problem!' + str(e))

def getIssues(s):
    url = "https://jira.org/rest/api/2/"
    headers = {
        'Authorization': "Basic amlyYXNkX2JvdDphc2RkKSlkYXMhZGUzMjQ=",
        'Cache-Control': "no-cache",
        'Postman-Token': "cfe71d04-4bd3-4d46-99a7-f38ea3828e6d",
        'Content-Type': 'application/json'
    }
    # jql для поиска тасков
    pool_jql = {
        "jql": s,
        "maxResults": 5000}
    case_pool = requests.request("GET", url + "search", headers=headers, params=pool_jql)
    case_pool_json = json.loads(case_pool.text)
    issues = case_pool_json["issues"]
    return issues

def main(delta):

    yesterday=datetime.now()-timedelta(delta)
    yesterday=yesterday.strftime('%d-%m-%Y')

    jql='project = MS AND issuetype = "Низкая скорость" and (createdDate<startOfDay('+str((delta-1)*-1)+') AND createdDate >startOfDay('+str(delta*-1)+'))'
    issuesCreated=getIssues(jql)
    jql='project = MS AND issuetype = "Низкая скорость" and (resolutiondate <startOfDay('+str((delta-1)*-1)+') AND resolutiondate >startOfDay('+str(delta*-1)+'))'
    issuesSolved=getIssues(jql)
    jql='project = MS AND issuetype = "Низкая скорость" and (resolutiondate <startOfDay('+str((delta-1)*-1)+') AND resolutiondate >startOfDay('+str(delta*-1)+')) AND assignee not in (jirasd_bot)'
    issuesSolvedByEngineer=getIssues(jql)
    jql='project = MS AND issuetype = "Низкая скорость" and (resolutiondate <startOfDay('+str((delta-1)*-1)+') AND resolutiondate >startOfDay('+str(delta*-1)+')) AND assignee in (jirasd_bot)'
    issuesSolvedByBot=getIssues(jql)

    if hasDate(yesterday):
        amountOfCreated=int(get_row(yesterday)['created'])+len(issuesCreated)
        amountOfSolved=int(get_row(yesterday)['solved'])+len(issuesSolved)
        amountOfSolvedByEngineer=int(get_row(yesterday)['solved_by_engineer'])+len(issuesSolvedByEngineer)
        amountOfSolvedByBot=int(get_row(yesterday)['solved_by_sms'])+len(issuesSolvedByBot)
        conn = db_connect()
        cur = conn.cursor()
        cur.execute('UPDATE other_jira."public".created_vs_solved SET solved = %s WHERE date = %s;',
                    (amountOfSolved,yesterday))
        cur.execute('UPDATE other_jira."public".created_vs_solved SET created = %s WHERE date = %s;',
                    (amountOfCreated, yesterday))
        cur.execute('UPDATE other_jira."public".created_vs_solved SET solved_by_sms = %s WHERE date = %s;',
                    (amountOfSolvedByBot, yesterday))
        cur.execute('UPDATE other_jira."public".created_vs_solved SET solved_by_engineer = %s WHERE date = %s;',
                    (amountOfSolvedByEngineer, yesterday))

        conn.commit()
        cur.close()
        conn.close()
    else:
        amountOfCreated =len(issuesCreated)
        amountOfSolved = len(issuesSolved)
        amountOfSolvedByEngineer = len(issuesSolvedByEngineer)
        amountOfSolvedByBot = len(issuesSolvedByBot)
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO other_jira."public".created_vs_solved '
            '(solved, solved_by_sms, solved_by_engineer, created, date) '
            'VALUES (%s, %s, %s, %s, %s)', (amountOfSolved, amountOfSolvedByBot, amountOfSolvedByEngineer, amountOfCreated, yesterday))
        conn.commit()
        cur.close()
        conn.close()

    print("It's OK!")

main(1)




