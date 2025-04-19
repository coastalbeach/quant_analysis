"""分析师相关数据获取模块

此模块用于获取和处理分析师相关的数据，包括：
- 分析师评级
- 研究报告
- 盈利预测
- 一致预期

主要功能：
1. 获取分析师评级数据
2. 获取研究报告内容和摘要
3. 获取分析师盈利预测数据
4. 获取市场一致预期数据

使用示例：
```python
from core.data.fetcher.analyst import get_analyst_ratings

# 获取某只股票的分析师评级
ratings = get_analyst_ratings('000001.SZ')
```
"""