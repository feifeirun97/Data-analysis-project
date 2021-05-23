import pandas as pd
import json
import re
import numpy as np
import matplotlib.pyplot as plt
import sys
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import mean_squared_error, average_precision_score, accuracy_score, recall_score, precision_score
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier

'''
Comp9321 Assn3
z5239235 Fei Xu
'''


def Top(val, process_column, new_column, top, plt_name, part):
    """
    :param val:            dataframe
    :param process_column: current processed column
    :param new_column:     new created column
    :param top:            top number
    :param plt_name:       name of image
    :param part:           part1 or part2
    :return: df
    """
    if process_column in ['cast', 'genres', 'production_companies', 'production_countries', 'spoken_languages']:
        f ='name'
        val[new_column] = val[process_column].apply(lambda a: [s[f] for s in json.loads(a)])
        tempdic = {}
        new_total = val[new_column].tolist()
        part_total = val[part].tolist()
        for i in range(len(val)):
            actors = list(set(new_total[i]))
            for j in actors:
                if j in tempdic:
                    tempdic[j] += part_total[i]
                else:
                    tempdic[j] = part_total[i]
        for i in sorted(tempdic.items(), key=lambda x: x[1])[-top:]:
            val[i[0]] = val[new_column].apply(lambda a: 0 if i[0] not in a else 1)

    elif process_column in ['crew']:
        x,y,z = 'name','job','Director'
        val[new_column] = val[process_column].apply(
            lambda a: [crew[x] for crew in json.loads(a) if crew[y] == z][0])

        tempdf = val.groupby(new_column)[part].sum().reset_index()
        tempdf = tempdf.sort_values(by=[part], ascending=[1])[-top:]
        for i in tempdf[new_column].tolist():
            val[i] = val[new_column].apply(lambda a: 0 if i not in a else 1)
    return val


def Df_process(csv_file, part):
    """
    :param csv_file: csv file
    :param part: part1 or part2
    :return: df
    """
    val = pd.read_csv(csv_file)
    val = Top(val, process_column='cast', new_column='total_actors', top=20, plt_name=f'top 20 {part} actors',
              part=part)
    val = Top(val, process_column='crew', new_column='directors', top=15, plt_name=f'top 15 {part} directors',
              part=part)
    val = Top(val, process_column='genres', new_column='total_genres', top=10, plt_name=f'top 10 {part} genres',
              part=part)
    val = Top(val, process_column='production_companies', new_column='total_companies', top=15,
              plt_name=f'top 15 {part} companies', part=part)
    val['homepage'] = val['homepage'].apply(lambda a: 0 if type(a) != str else 1)

    val['original_language'] = val['original_language'].apply(lambda a: 0 if a != 'en' else 1)
    val['release_date'] = val['release_date'].apply(
        lambda a: 1 if int(re.findall(r'-(\d+)-', a)[0]) in [6, 12, 6, 11, 7] else 0)
    val['production_countries'] = val['production_countries'].apply(
        lambda a: 0 if 'United States of America' not in [s['name'] for s in json.loads(a)] else 1)

    val.drop(['cast', 'crew', 'genres', 'keywords', 'tagline', 'status', 'original_title', 'overview',
              'production_companies', 'spoken_languages'], axis=1, inplace=True)
    val.drop(['total_actors', 'directors', 'total_genres', 'total_companies'], axis=1, inplace=True)

    return val


def Pred(train, val, part):
    """
    :param train: df for train
    :param val: df for prediction
    :param part: part1 or part2
    :return: z5239235.PART1.summary.csv
            z5239235.PART1.output.csv
            z5239235.PART2.summary.csv
            z5239235.PART2.output.csv'
    """
    x_T = train.drop(['revenue', 'rating', 'movie_id'], axis=1).values.astype(int)
    y_T = train[part].values
    x_Val = val.drop(['revenue', 'rating', 'movie_id'], axis=1).values.astype(int)
    y_Val = val[part].values
    ids = val['movie_id'].values
    if part == 'revenue':
        ml = RandomForestRegressor(n_estimators=30, random_state=60)
        ml.fit(x_T, y_T)
        y_Pred = ml.predict(x_Val)
        mse = mean_squared_error(y_Val, y_Pred)
        cor = np.corrcoef(y_Val, y_Pred)[0, 1]
        dic1 = {'zid': 'z5239235', 'MSE': int(mse), 'correlation': '%.2f' % cor}
        dic2 = {'movie_id': ids, 'predicted_revenue': [int(i) for i in y_Pred]}

        pd.DataFrame(dic1, index=[0]).to_csv('z5239235.PART1.summary.csv', index=False)
        pd.DataFrame(dic2).sort_values(by=['movie_id'],ascending=[1]).to_csv('z5239235.PART1.output.csv', index=False)

    elif part == 'rating':
        x_T = train.drop(['revenue', 'rating', 'movie_id'], axis=1)
        y_T = train['rating'].values
        x_Val = val.drop(['revenue', 'rating', 'movie_id'], axis=1).values.astype(int)
        y_Val = val['rating'].values
        ids = val['movie_id'].values
        ml = RandomForestClassifier(n_estimators=30, random_state=17)
        ml.fit(x_T, y_T)
        y_Pred = ml.predict(x_Val)
        acc = accuracy_score(y_Val, y_Pred)
        pre = precision_score(y_Val, y_Pred, average='macro')
        rec = recall_score(y_Val, y_Pred, average='macro')
        dic1 = {'zid': 'z5239235', 'average_precision': '%.2f' % pre, 'average_recall': '%.2f' % rec,
                'accuracy': '%.2f' % acc}
        dic2 = {'movie_id': ids, 'predicted_rating': [int(k) for k in y_Pred]}

        pd.DataFrame(dic1, index=[0]).to_csv('z5239235.PART2.summary.csv', index=False)
        pd.DataFrame(dic2).sort_values(by=['movie_id'],ascending=[1]).to_csv('z5239235.PART2.output.csv', index=False)


if __name__ == '__main__':
    try:
        train_file, test_file = sys.argv[1:][0], sys.argv[1:][1]
        train = Df_process(train_file, part='revenue')
        test = Df_process(test_file, part='revenue')
        Pred(train=train, val=test, part='revenue')
        train = Df_process(train_file, part='rating')
        test = Df_process(test_file, part='rating')
        Pred(train=train, val=test, part='rating')
    except OSError as e:
        print({'msg': e})
