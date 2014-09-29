# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd

today = datetime.now().strftime('%Y-%m-%d')


def get_results():

    csv = pd.DataFrame.from_csv('10years-survey.csv', header=0, index_col=False, parse_dates=False)
    csv = csv.dropna(subset=['Unnamed: 1'])
    survey = pd.DataFrame({
        'timestamp': pd.Series(csv['時間戳記']).convert_objects(convert_dates='coerce'),
    })
    survey['is_dev'] = pd.Series(csv['Unnamed: 1'] == '是', dtype='bool')
    survey['not_dev'] = ~survey['is_dev']

    survey['online_activities'] = pd.Series(csv['Unnamed: 8'])
    survey['online_activities'] = survey['online_activities'].fillna('')
    survey['online_activities_search'] = pd.Series(survey['online_activities'].str.contains('查詢資料、文件、翻譯、地圖'), dtype='bool')
    survey['online_activities_read'] = pd.Series(survey['online_activities'].str.contains('看新聞和部落文'), dtype='bool')
    survey['online_activities_social'] = pd.Series(survey['online_activities'].str.contains('上社群網站、聊天、收發信件'), dtype='bool')
    survey['online_activities_shop'] = pd.Series(survey['online_activities'].str.contains('上網購物'), dtype='bool')
    survey['online_activities_entertain'] = pd.Series(survey['online_activities'].str.contains('看影片、聽音樂、玩遊戲'), dtype='bool')
    survey['online_activities_debug'] = pd.Series(survey['online_activities'].str.contains('進行網頁除錯'), dtype='bool')

    other_activities = survey['online_activities'].str.replace(
        '查詢資料、文件、翻譯、地圖[,]?|看新聞和部落文[,]?|上社群網站、聊天、收發信件[,]?|'
        '上網購物[,]?|看影片、聽音樂、玩遊戲[,]?|進行網頁除錯[,]?', '')
    other_activities = other_activities.str.strip()
    other_activities = other_activities[other_activities.str.len() > 0]
    survey['online_activities_other'] = other_activities

    survey['firefox'] = pd.Series(csv['Firefox'], dtype='int')
    survey['chrome'] = pd.Series(csv['Chrome'], dtype='int')
    survey['ie'] = pd.Series(csv['Internet Explorer'], dtype='int')
    survey['safari'] = pd.Series(csv['Safari'], dtype='int')
    survey['opera'] = pd.Series(csv['Opera'], dtype='int')
    survey['speed'] = pd.Series(csv['速度快'], dtype='int')
    survey['security'] = pd.Series(csv['安全性高'], dtype='int')
    survey['privacy'] = pd.Series(csv['保護隱私'], dtype='int')
    survey['customize'] = pd.Series(csv['可自訂性'], dtype='int')
    survey['interface'] = pd.Series(csv['介面設計'], dtype='int')
    survey['support'] = pd.Series(csv['支援最新開放標準'], dtype='int')
    survey['stable'] = pd.Series(csv['穩定性'], dtype='int')
    survey['sync'] = pd.Series(csv['書籤同步功能'], dtype='int')
    survey['autocomplete'] = pd.Series(csv['表單自動填寫功能'], dtype='int')
    survey['opensource'] = pd.Series(csv['開放原始碼'], dtype='int')
    survey['other'] = pd.Series(csv['其他要件'])
    survey['fx_speed'] = pd.Series(csv['速度'], dtype='int')
    survey['fx_security'] = pd.Series(csv['安全性'], dtype='int')
    survey['fx_privacy'] = pd.Series(csv['保護隱私.1'], dtype='int')
    survey['fx_customize'] = pd.Series(csv['自訂性'], dtype='int')
    survey['fx_interface'] = pd.Series(csv['介面設計.1'], dtype='int')
    survey['fx_support'] = pd.Series(csv['開放標準支援度'], dtype='int')
    survey['fx_stable'] = pd.Series(csv['穩定性.1'], dtype='int')
    survey['fx_sync'] = pd.Series(csv['書籤同步功能.1'], dtype='int')
    survey['fx_autocomplete'] = pd.Series(csv['表單自動填寫功能.1'], dtype='int')
    survey['fx_opensource'] = pd.Series(csv['開放原始碼.1'], dtype='int')

    survey['fx_impression'] = pd.Series(csv['Unnamed: 14'])
    survey['fx_impression'] = survey['fx_impression'].fillna('')
    survey['fx_impression_speed'] = pd.Series(survey['fx_impression'].str.contains('速度快'), dtype='bool')
    survey['fx_impression_security'] = pd.Series(survey['fx_impression'].str.contains('安全性高'), dtype='bool')
    survey['fx_impression_privacy'] = pd.Series(survey['fx_impression'].str.contains('保護個人隱私'), dtype='bool')
    survey['fx_impression_customize'] = pd.Series(survey['fx_impression'].str.contains('自訂性高'), dtype='bool')
    survey['fx_impression_opensource'] = pd.Series(survey['fx_impression'].str.contains('開放原始碼'), dtype='bool')
    other_impression = survey['fx_impression'].str.replace(
        '速度快[,]?|安全性高[,]?|保護個人隱私[,]?|自訂性高[,]?|開放原始碼[,]?', '')
    other_impression = other_impression.str.strip()
    other_impression = other_impression[other_impression.str.len() > 0]
    survey['fx_impression_other'] = other_impression

    survey['email'] = pd.Series(csv['Unnamed: 31'].str.strip())
    survey['subscribe'] = pd.Series(csv['Unnamed: 32'] == '我想訂閱 Firefox 電子報', dtype='bool')
    survey['valid'] = pd.Series(dtype='bool')
    survey['valid'] = survey['valid'].fillna(True)

    survey = survey.dropna(subset=['email', 'firefox', 'chrome', 'ie', 'safari', 'opera',
                                   'speed', 'security', 'privacy', 'customize', 'interface',
                                   'support', 'stable', 'sync', 'autocomplete', 'opensource'])

    non_fx_user_survey = survey[survey['firefox'] == 1]
    fx_user_survey = survey[survey['firefox'] > 1].dropna(
        subset=['fx_speed', 'fx_security', 'fx_privacy', 'fx_customize', 'fx_interface',
                'fx_support', 'fx_stable', 'fx_sync', 'fx_autocomplete', 'fx_opensource'])
    survey = fx_user_survey.append(non_fx_user_survey)

    def drop_all_duplicates(group):
        if len(group.index) > 1:
            group = None
        return group
    survey = survey.groupby('email', sort=False, as_index=False, group_keys=False).apply(drop_all_duplicates)
    # print survey
    summary = pd.DataFrame({
        'all': pd.Series(survey.sum(), dtype='float32')
    })
    summary['is_dev'] = pd.Series(survey[survey['is_dev']].sum(), dtype='float32')
    summary['not_dev'] = pd.Series(survey[~survey['is_dev']].sum(), dtype='float32')
    summary['search'] = pd.Series(survey[survey['online_activities_search']].sum(), dtype='float32')
    summary['no_search'] = pd.Series(survey[~survey['online_activities_search']].sum(), dtype='float32')
    summary['read'] = pd.Series(survey[survey['online_activities_read']].sum(), dtype='float32')
    summary['no_read'] = pd.Series(survey[~survey['online_activities_read']].sum(), dtype='float32')
    summary['social'] = pd.Series(survey[survey['online_activities_social']].sum(), dtype='float32')
    summary['no_social'] = pd.Series(survey[~survey['online_activities_social']].sum(), dtype='float32')
    summary['shop'] = pd.Series(survey[survey['online_activities_shop']].sum(), dtype='float32')
    summary['no_shop'] = pd.Series(survey[~survey['online_activities_shop']].sum(), dtype='float32')
    summary['entertain'] = pd.Series(survey[survey['online_activities_entertain']].sum(), dtype='float32')
    summary['no_entertain'] = pd.Series(survey[~survey['online_activities_entertain']].sum(), dtype='float32')
    summary['debug'] = pd.Series(survey[survey['online_activities_debug']].sum(), dtype='float32')
    summary['no_debug'] = pd.Series(survey[~survey['online_activities_debug']].sum(), dtype='float32')
    summary['fx_1'] = pd.Series(survey[survey['firefox'] == 1].sum(), dtype='float32')
    summary['fx_2'] = pd.Series(survey[survey['firefox'] == 2].sum(), dtype='float32')
    summary['fx_3'] = pd.Series(survey[survey['firefox'] == 3].sum(), dtype='float32')
    summary['fx_4'] = pd.Series(survey[survey['firefox'] == 4].sum(), dtype='float32')
    summary['fx_5'] = pd.Series(survey[survey['firefox'] == 5].sum(), dtype='float32')
    summary['fx_6'] = pd.Series(survey[survey['firefox'] == 6].sum(), dtype='float32')
    summary['fx_7'] = pd.Series(survey[survey['firefox'] == 7].sum(), dtype='float32')
    summary['gc_1'] = pd.Series(survey[survey['chrome'] == 1].sum(), dtype='float32')
    summary['gc_2'] = pd.Series(survey[survey['chrome'] == 2].sum(), dtype='float32')
    summary['gc_3'] = pd.Series(survey[survey['chrome'] == 3].sum(), dtype='float32')
    summary['gc_4'] = pd.Series(survey[survey['chrome'] == 4].sum(), dtype='float32')
    summary['gc_5'] = pd.Series(survey[survey['chrome'] == 5].sum(), dtype='float32')
    summary['gc_6'] = pd.Series(survey[survey['chrome'] == 6].sum(), dtype='float32')
    summary['gc_7'] = pd.Series(survey[survey['chrome'] == 7].sum(), dtype='float32')
    summary['ie_1'] = pd.Series(survey[survey['ie'] == 1].sum(), dtype='float32')
    summary['ie_2'] = pd.Series(survey[survey['ie'] == 2].sum(), dtype='float32')
    summary['ie_3'] = pd.Series(survey[survey['ie'] == 3].sum(), dtype='float32')
    summary['ie_4'] = pd.Series(survey[survey['ie'] == 4].sum(), dtype='float32')
    summary['ie_5'] = pd.Series(survey[survey['ie'] == 5].sum(), dtype='float32')
    summary['ie_6'] = pd.Series(survey[survey['ie'] == 6].sum(), dtype='float32')
    summary['ie_7'] = pd.Series(survey[survey['ie'] == 7].sum(), dtype='float32')
    summary['op_1'] = pd.Series(survey[survey['opera'] == 1].sum(), dtype='float32')
    summary['op_2'] = pd.Series(survey[survey['opera'] == 2].sum(), dtype='float32')
    summary['op_3'] = pd.Series(survey[survey['opera'] == 3].sum(), dtype='float32')
    summary['op_4'] = pd.Series(survey[survey['opera'] == 4].sum(), dtype='float32')
    summary['op_5'] = pd.Series(survey[survey['opera'] == 5].sum(), dtype='float32')
    summary['op_6'] = pd.Series(survey[survey['opera'] == 6].sum(), dtype='float32')
    summary['op_7'] = pd.Series(survey[survey['opera'] == 7].sum(), dtype='float32')
    summary['sa_1'] = pd.Series(survey[survey['safari'] == 1].sum(), dtype='float32')
    summary['sa_2'] = pd.Series(survey[survey['safari'] == 2].sum(), dtype='float32')
    summary['sa_3'] = pd.Series(survey[survey['safari'] == 3].sum(), dtype='float32')
    summary['sa_4'] = pd.Series(survey[survey['safari'] == 4].sum(), dtype='float32')
    summary['sa_5'] = pd.Series(survey[survey['safari'] == 5].sum(), dtype='float32')
    summary['sa_6'] = pd.Series(survey[survey['safari'] == 6].sum(), dtype='float32')
    summary['sa_7'] = pd.Series(survey[survey['safari'] == 7].sum(), dtype='float32')

    summary = summary.transpose()
    for column in summary.columns:
        summary[column] /= summary['valid']

    print summary.dtypes

    return {
        'survey': survey,
        'summary': summary,
    }


def save_results(results):
    if 'survey' in results:
        survey = results['survey']
        print '%d rows fetched' % len(survey)
        survey.to_hdf('browser_survey.h5', 'survey')
    else:
        print 'No survey results found'

    if 'summary' in results:
        summary = results['summary']
        summary.to_csv('10years-survey-summary.csv')
        summary.to_hdf('browser_survey.h5', 'summary')
    else:
        print 'No summary results found'


def main():
    # Step 1. Get an analytics service object.
    try:
        # Step 3. Query the Core Reporting API.
        results = get_results()
        # for result, value in results.items():
        #     print result + ': ' + str(value)
        # print len(results)
        # Step 4. Output the results.
        save_results(results)

    except TypeError, error:
        # Handle errors in constructing a query.
        print ('There was an error in constructing your query : %s' % error)


# main(sys.argv)
