import base64
from db_user_settings import CREDENTIALS
from collections import namedtuple
# Bb learn is hard limited to 100 max in return results
THREAD_MULTIPLIER = 10
PAGINATION = 5
DEFAULT_MODE = 'prod'
term = 20203
termId = 221
terms = [
    '20203',
    '20203A',
    '20203B',
]
termIds = [
    '221',
]
MODES = ['test', 'dev', 'staging', 'prod']
config = {
    'mongo': {
        'dev': {
            'url': 'mongodb://localhost:27017/?replicaSet=rs01',
            'database': 'bb_devcon_20_dev',
        },
        'test': {
            'url': 'mongodb://localhost:27017/?replicaSet=rs01',
            'database': 'bb_devcon_20_test',
        },
        'staging': {
            'url': 'mongodb://localhost:27017/?replicaSet=rs01',
            'database': 'bb_devcon_20_staging',
        },
        'prod': {
            'url': 'mongodb://localhost:27017/?replicaSet=rs01',
            'database': 'bb_devcon_20',
        }
    },
    'bblearn': {
        'dev': 'https://irsc-saastest.blackboard.com',
        'test': 'https://irsc-saastest.blackboard.com',
        'staging': 'https://irsc-staging.blackboard.com',
        'prod': 'https://irsc.blackboard.com',
        'token': '',
        'grantType': 'client_credentials',
        'key': CREDENTIALS['bblearn'][DEFAULT_MODE]['key'],
        'secret': CREDENTIALS['bblearn'][DEFAULT_MODE]['secret'],
        'tokenKey': 'access_token',
        "apiVersion": 'v1',
        "api": {
            "announcements": {
                "path": "/learn/api/public/v1/announcements",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": "title,body,showAtLogin,showInCourses,availability,id"
                },
                "replace": "{announcementId}"
            },
            "courses": {
                "path": "/learn/api/public/v2/courses",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": "externalId,courseId,name,availability,id"
                },
                "replace": "{courseId}"
            },
            "contents": {
                "path": "/learn/api/public/v1/courses/{courseId}/contents",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": "id,parentId,title,body,description,created,availability"
                },
                "replace": "{courseId}"
            },
            "users": {
                "path": "/learn/api/public/v1/users",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": "id,uuid,externalId,userName,name,contact"
                },
                "replace": '{userId}'
            },
            "terms": {
                "path": "/learn/api/public/v1/terms",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": "externalId,name"
                },
                "replace": '{termId}'
            },
            "systems": {
                "path": "/learn/api/public/v1/system/version",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": ""
                },
                "replace": '{systemId}'
            },
            "memberships": {
                "path": "/learn/api/public/v1/courses/{courseId}/users",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": ""
                },
                "replace": '{courseId}'
            },
            "grades": {
                "path": "/learn/api/public/v1/courses/{courseId}/gradebook/columns",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": ""
                },
                "replace": '{courseId}'
            },
            "datasources": {
                "path": "/learn/api/public/v1/dataSources",
                "params": {
                    "offset": 0,
                    "limit": PAGINATION,
                    "fields": "externalId,name"
                },
                "replace": ''
            },
            "set_token": {
                "path": "/learn/api/public/v1/oauth2/token"
            }
        },
    },
    'bbdata': CREDENTIALS['bbdata']
}


Roles = namedtuple('Roles', [
    'Student',
    'Faculty',
    'Faculty_VC',
    'Guest',
    'TeachingAssistant',
    'TLS_Faculty',
    'TLS_Faculty_VC',
    'Instructor',
    'Grader',
    'BB_FACILITATOR',
    'CourseBuilder'
])
Roles.Student = 'Student'
Roles.Faculty = 'F'
Roles.Faculty_VC = 'VCF'
Roles.Guest = 'U'
Roles.TeachingAssistant = 'T'
Roles.TLS_Faculty = 'TLSF'
Roles.TLS_Faculty_VC = 'TLSVCF'
Roles.Instructor = 'Instructor'
Roles.Grader = 'G'
Roles.BB_FACILITATOR = 'Facilitator'
Roles.CourseBuilder = 'B'

Availability = namedtuple('Yes', ['Yes', 'No'],)
Availability.Yes = 'Yes'
Availability.No = 'No'

course_roles = [
    'F',
    'Faculty',
    'VCF',
    'S',
    'Student'
]

included_roles = [
    'F',
    'Faculty',
    'VCF',
    'S',
    'Student'
]

excluded_roles = [
    'CourseBuilder',
    'B',
    'Facilitator',
    'BB_FACILITATOR',
    'EEI',
    'G',
    'Grader',
    'P',
    'Insructor',
    'TLSF',
    'TLSVCF',
    'T',
    'TeachingAssistant',
    'U',
    'Guest',
]

