# Data-analysis-project
Using pandas,numpy, restful api, sqlite and  meachine learing.

### Data service for TVshows

通过爬取 tv maze api取得数据，清洗脏数据，建立本地数据库后，开发Flask-Rest数据服务，该服务允许客户端读取和储存一些电视节目，并允许消费者通过REST API访问数据，并针部分数据实现可视化。

`The source URL: (http://api.worldbank.org/v2/) http://api.tvmaze.com/shows (http://api.tvmaze.com/shows)
(http://api.worldbank.org/v2/) Documentations on API Call Structure: https://www.tvmaze.com/api (https://www.tvmaze.com/api)`

### Predictions on movie revenues and rating

建立一个可以通过电影的演员阵容，导演，语言，出品公司/国家等数据就可以预测电影票房和电影评价的系统。对一个庞大的电影数据集，将其分成两部分一个作训练集另一个作测试集。筛去脏数据后，通过机器学期模型来预测电影票房和电影评价，包括了随机森林回归和KNN分类模型。

