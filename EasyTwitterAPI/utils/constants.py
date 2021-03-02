class Cte:
    CONF_DB = 'confdb'
    EXPERT_DB = 'expertdb'
    HASH_DB = 'hashtagdb'

    FOLLOWEES = 'followees'
    FOLLOWERS = 'followers'
    FRIENDS = 'friends'

    ANSWER = 'answer'
    TWEET = 'tweet'
    RETWEET = 'retweet'
    QTWEET = 'qtweet'
    LIKE = 'like'

    RATE_LIMIT = 429
    OVERLOAD = 503
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    INTERNAL_ERROR = 500

    LIST_OWN = 'o'
    LIST_MEMBER = 'm'
    LIST_SUSC = 's'

    TRACKER = None
    COUNTER = {0: 0, 1: 0, 2: 0}
    PRINT = False

    SEC = 'sec'
    MIN = 'min'
    HOUR = 'h'

