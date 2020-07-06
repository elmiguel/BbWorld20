"""Workday To Blackboard Sync API 0.0.5
Usage:
    new-wd-course-sync.py [options]
    new-wd-course-sync.py (-h | --help)
    new-wd-course-sync.py --version

Commands:
    -s <store>, --store <store>  Set the persistence layer. [default: mongo]

Options:
General Options:
    -h, --help      Show this screen.
    -v, --version   Show version.
    -D, --debug     Enable debug mode: verbose.
    --pprint        Pretty print the results.
"""
import csv
import json
import requests
import multiprocessing
import sys
import re
import rx

from rx import operators as ops
from rx.scheduler import ThreadPoolScheduler
from settings import config as config, course_ids, term, termId, vc_courses, DEFAULT_MODE, THREAD_MULTIPLIER
from docopt import docopt
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient, ReturnDocument, ASCENDING
from pprint import pprint


student_pattern = re.compile(r"[A-Za-z]{1}\d{8}", re.DOTALL)


def to_dict(result):
    return [dict(zip(row.keys(), row)) for row in result]


def load_active_courses_query():
    with open('./queries/old/ESSQL3_Active_Courses.sql') as cms:
        query = cms.read().replace('{term}', str(term))
        return to_dict(sessions['essql3'].execute(query))

def load_cm_instructors():
    with open('./imports/cm_instructor_roles.csv') as cms:
        data = cms.read().split('\n')[1:]
        # print(data)
        return data


def load_course_memberships_query():
    with open('./queries/BBLEARN_Course_Memberships.sql') as cms:
        query = cms.read().replace('{term}', str(term))
        return to_dict(sessions['bbdda'].execute(query))


def load_course_ids_query():
    with open('./queries/BBLEARN_CourseIDs.sql') as cids:
        query = cids.read().replace('{term}', str(term))
        return to_dict(sessions['bbdda'].execute(query))


def load_vcf_query():
    with open('./queries/BBLEARN_VCF_Instructors.sql') as vcfi:
        query = vcfi.read().replace('{term}', str(term)).replace(
            '{vc_courses}', vc_courses)
        return to_dict(sessions['bbdda'].execute(query))


def get_course_enrollments(course):
    course_parts = course.split('-')
    courseId = course_parts[0]
    term = course_parts[1]
    campus = course_parts[2]
    session = course_parts[3]
    section = course_parts[4]
    _course = f"{courseId}-{campus}-{section}"
    academic_period = f"{term}{session if session is not 'M' else ''}"
    course_url = f"{url}?Section={_course}&Academic_Period!Academic_Period_ID=ACADEMIC_PERIOD_{academic_period}&format=json"
    # print(course_url)
    response = s.get(course_url)
    data = response.json()
    data['Report_Entry'][0]['external_course_key'] = course
    # pprint(data)
    # sys.exit(1)
    return [course, data]


def is_student(username):
    return student_pattern.search(username)


def determine_role(course_id, username):
    role = 'F'
    # print('=' * 30)
    # print('DETERMINE ROLE!!!!')

    # print('-' * 30)
    # print('USERNAME =>', username)
    # print('-' * 30)

    if is_student(username):
        # print('student account found')
        # print(course_id, username)
        role = 'S'

    # print('*' * 30)
    # print('NOT a student')
    # print('*' * 30)
    if '1V' in course_id and course_id.split('-')[0] in course_ids:
        # print(course_id, username)
        role = 'VCF'

    # print('~' * 30)
    # print('REGULAR Instructor')
    # print('=' * 30)
    return role


def process_course_enrollments(course, data):
    # print(json.dumps(data, indent=2))
    wd_enrollments = []
    try:
        wd_enrollments = data['Report_Entry'][0]['enrollments']
    except KeyError as ke:
        print(f"No Enrollments for: {course}")
        # print('Missing Enrollments Key======>')
        # print(json.dumps(data, indent=2))
        # print('=' * 80)
    # print(json.dumps(wd_enrollments, indent=2))

    # pull out current course bb enrollments
    # print('# pull out current course bb enrollments')
    bb_course_memberships = []
    try:
        bb_course_memberships = list(filter(
            lambda m: m['external_course_key'] == course,
            memberships
        ))
    except KeyError as ke:
        print(f"No External Id Key: {course}")
        # print('Missing External Id Key======>')
        # print(json.dumps(data, indent=2))
        # print('='*80)

    # print(json.dumps(bb_course_memberships, indent=2))

    def __get_correct_user_name__(user):
        return user['external_person_key'] if user['courseRoleId'] == 'S' else user['userName']
    # create simple lists of rosters
    # print('# create simple lists of rosters')
    bb_user_lookup = {__get_correct_user_name__(be): be['external_person_key']
                      for be in bb_course_memberships}

    # TODO: if primary or seconday instructor in Workday is 
    # also in Bb and the Bb role is instructor, it will cause
    # the enrollments to disable secondary instructors


    # add primary instructor
    try:
        wd_enrollments += [{
            "userName": data['Report_Entry'][0]['primaryInstructorEmail'].replace('@irsc.edu', ''),
            "status": "Registered"
        }]
    except KeyError as ke:
         print(f"{ke} key was not found in data object.")


    # add secondary insructors, if any
    
    try:
        wd_enrollments += [
            {
                "userName": instructor['email'].replace('@irsc.edu', ''),
                "status": "Registered"
            }
            for instructor in data['Report_Entry'][0]['secondaryInstructors']
        ]
    except KeyError as ke:
        print(f"{ke} key was not found in data object.")

    bb_roster = [be['external_person_key'] for be in bb_course_memberships]

    def __get_user_name__(user):
        # print('*' * 30)
        # print('[__get_user_name__] ==/>')
        # print(user)
        # print('*' * 30)
        try:
            return user['userName']
        except KeyError as ke:
            return user['studentId']

    def __setup_wd_roster__(roster):
        for e in wd_enrollments:
            roster += [__get_user_name__(e)]
        return roster

    wd_roster = __setup_wd_roster__([])

    # sys.exit(1)
    # print('[PROCESSING DROPS...........]')

    # filter out the drops
    drops = list(map(
        lambda m: {
            'external_course_key': m['external_course_key'],
            'external_person_key': m['external_person_key'],
            'role': m['courseRoleId'],
            'available_ind': 'N',
            'data_source_key': 'MARINER_SIS'
        },
        list(filter(
            lambda m: __get_correct_user_name__(
                m) not in wd_roster and m['availability']['available'] == 'Yes' and m['courseRoleId'] != 'P',
            bb_course_memberships
        ))
    ))

    # print('[PROCESSING ADDS...........]')

    # print('[BB_ROSTER_LOOKUP]', bb_user_lookup)
    # print('[WD_ROSTER]', wd_roster)
    # print('[WD_ENROLLMENTS]', wd_enrollments)
    # print('[BB_ROSTER]', bb_roster)
    # print('[BB_COURSE_MEMBERSHIPS]', bb_course_memberships)

    # print(json.dumps(enrollments, indent=2))
    # fitler out the adds and update return object

    adds = list(map(
        lambda m: {
            'external_course_key': course,
            'external_person_key': bb_user_lookup[__get_user_name__(m)],
            'role': determine_role(course, __get_user_name__(m)),
            'available_ind': 'Y',
            'data_source_key': 'MARINER_SIS'
        },
        list(filter(
            lambda m: bb_user_lookup[__get_user_name__(
                m)] not in bb_roster,
            wd_enrollments
        ))
    ))


    # double checks for instructors who are in as an instructor
    # but need to have the correct role according to workday

    # print('[PROCESSING ADDS2...........]')
    def __get_bb_course_record__(user):
        _user = list(filter(
            lambda u: u['userName'].lower() == __get_user_name__(user).lower(),
            bb_course_memberships
        ))[0]
        # print('[__get_bb_course_record__]: ==> ', _user)
        return _user


    adds2 = list(map(
        lambda m: {
            'external_course_key': course,
            'external_person_key': bb_user_lookup[__get_user_name__(m)],
            'role': determine_role(course, __get_user_name__(m)),
            'available_ind': 'Y',
            'data_source_key': 'MARINER_SIS'
        },
        list(filter(
            lambda m: bb_user_lookup[__get_user_name__(m)] in bb_roster and __get_bb_course_record__(m)['courseRoleId'] == 'P',
            wd_enrollments
        ))
    ))

    # merge the two add lists
    adds += adds2

    # TODO FIX ABOVE SNIPPET AND BELOW AS IT NOT 100%
    # print('[PROCESSING ADDS...........]')
    # print('[-----> ADDS]',
    #       list(map(lambda m: __get_user_name__(m), list(
    #           filter(lambda m: __get_user_name__(m) not in bb_roster, wd_enrollments))))
    #       )
    # adds = list(map(
    #     lambda m: {
    #         'external_course_key': course,
    #         'external_person_key': __get_user_name__(m),
    #         'external_person_key': __get_user_name__(m),
    #         'role': determine_role(course, __get_user_name__(m)),
    #         'available_ind': 'Y',
    #         'data_source_key': 'MARINER_SIS'
    #     },
    #     list(
    #         filter(lambda m: __get_user_name__(m) not in bb_roster, wd_enrollments))
    # ))

    # print('[PROCESSING ENROLLMENT DIFF..................]')
    # merge adds and drop, send drops first then adds
    # print('[DROPS]', drops)
    # print('[ADDS]', adds)
    enrollment_diff = drops + adds
    # print('#' * 30)
    # print('ENROLLMENT DIFF!!!!!!!')
    # print(json.dumps(enrollment_diff, indent=2))
    # print('#' * 30)
    # print('AM I EVEN GETTING CALLED????')
    return [course, enrollment_diff]


def send_course_enrollments(course, data):
    if data != []:
        with open(output_file, 'a') as wd:
            for e in data:
                wd.write(
                    f"{e['external_course_key']}|{e['external_person_key']}|{e['role']}|{e['available_ind']}|{e['data_source_key']}\n"
                )

    # TODO: update to return bb payload
    return [course, data]


def add_instructor_to_file(instructor):
    e = instructor
    with open(output_file, 'a') as wd:
        wd.write(
            f"{e['external_course_key']}|{e['external_person_key']}|{e['role']}|{e['available_ind']}|{e['data_source_key']}\n"
        )

def convert_instructor_line(instructor) :
    instructor = instructor.split('|')
    return {
        'external_course_key': instructor[0],
        'external_person_key': instructor[1],
        'role': instructor[2],
        'available_ind': instructor[3],
        'data_source_key': instructor[4],
    }

def main():
    # first create the output_file

    # calculate number of CPU's, then create a ThreadPoolScheduler with that number of threads
    optimal_thread_count = multiprocessing.cpu_count() * THREAD_MULTIPLIER
    pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

    [
        rx.just(course).pipe(
            ops.map(lambda c: get_course_enrollments(c['batch_uid'])),
            # c[0] = course batch_uid, c[1] = course roster data
            ops.map(lambda c: process_course_enrollments(c[0], c[1])),
            # c[0] = course batch_uid, c[1] = new course roster data,
            ops.map(lambda c: send_course_enrollments(c[0], c[1])),
            ops.map(lambda c: pprint(c)),
            ops.subscribe_on(pool_scheduler)
        )
        .subscribe(on_next=lambda data: print('PROCESSING...', data),
                   on_error=lambda e: print(e),
                   on_completed=lambda: print('PROCESS Memberships done!'))
        for course in courses
    ]

    # add the vcf instructors to the file
    if process_vcf:
        if vcf_instructors != []:
            [
                rx.just(instructor).pipe(
                    ops.map(lambda i: add_instructor_to_file(i)),
                    ops.subscribe_on(pool_scheduler)
                )
                .subscribe(on_next=lambda data: print('PROCESSING...', data),
                           on_error=lambda e: print(e),
                           on_completed=lambda: print('PROCESSING INSTRUCTOR done!'))
                for instructor in vcf_instructors
            ]
    
    if process_cms:
        if cm_instructors != []:
            [
                rx.just(instructor).pipe(
                    ops.map(lambda i: convert_instructor_line(i)),
                    ops.map(lambda i: add_instructor_to_file(i)),
                    ops.subscribe_on(pool_scheduler)
                )
                .subscribe(on_next=lambda data: print('PROCESSING...', data),
                           on_error=lambda e: print(e),
                           on_completed=lambda: print('PROCESSING CM INSTRUCTOR done!'))
                for instructor in cm_instructors
            ]

    print('Job Complete!!')


if __name__ == '__main__':

    args = docopt(__doc__, version='Workday To Blackboard Sync API 0.0.5')

    store = args['--store']
    debug = args['--debug']

    client = None
    if store == 'mongo':
        # connect to mongo
        client = MongoClient(config['mongo'][DEFAULT_MODE]['url'])
        mongo_db = client[config['mongo'][DEFAULT_MODE]['database']]

    engines = {
        'bbdda': create_engine(
            f"postgres://{config['bbdda']['user']}:{config['bbdda']['pass']}@{config['bbdda']['host']}:{config['bbdda']['port']}/{config['bbdda']['database']}",
            echo=debug
        )
    }

    sessions = {
        'bbdda': sessionmaker(bind=engines['bbdda'])(),
    }
    process_vcf = True
    process_cms = True
    courses = load_course_ids_query()
    print(f'Retrieved: {len(courses)} courses from Bb Data')
    memberships = [json.loads(m['results'])
                   for m in load_course_memberships_query()]
    print(f'Retrieved: {len(memberships)} memberships from Bb Data')
    vcf_instructors = load_vcf_query()
    cm_instructors = load_cm_instructors()
    print(f'Retrieved: {len(cm_instructors)} cm_instructors from Data File')
    print(f'Retrieved: {len(vcf_instructors)} vcf_instructors from Bb Data')

    for db, session in sessions.items():
        session.close()

    output_file = './exports/ActiveStudentsFromWD-BBDDA.csv'
    log_file = './exports/ErrorLog.csv'
    no_enroll_file = './exports/NoEnrollments.csv'

    url = f"{config['workday'][DEFAULT_MODE]['url']}/{config['workday'][DEFAULT_MODE]['defaultReportUser']}/{config['workday']['reports']['registeredStudents']}"

    s = requests.Session()
    s.auth = (config['workday']['key'], config['workday']['secret'])
    with open(output_file, 'w') as wd:
        wd.write(
            'external_course_key|external_person_key|role|available_ind|data_source_key\n')
    # TEST
    # pprint(courses[:1])
    # process_vcf = False
    # process_cms = False
    # courses = list(filter(
    #     lambda c: c['batch_uid'] in [
    #         'AMH2020-20203-1V-A-005',
    #         # 'SLS1101-20203-1V-A-024'
    #         # 'NUR4655-20203-1V-A-001',
    #         # 'NUR4837-20203-1V-M-001'
    #         # 'NUR4827-20203-1V-M-001',
    #         # 'MGF2106-20203-1V-A-030',
    #         # 'HUS2820-20203-11-A-001',
    #         # 'HUS3351-20203-11-A-001',
    #         # 'PLA2223-20203-11-A-001',
    #         # 'CJL2500-20203-11-A-001',
    #         # 'HCP0410C-20202-71-M-006',
    #         # 'CCJ4054-20202-11-M-002',
    #         # 'ENC1101-20202-31-M-007',
    #         # 'HSA3113-20202-1V-M-001',
    #         # 'HSA3113-20202-1V-M-002',
    #         # 'BSC2010L-20202-51-M-004',
    #         # 'GED1050-20202-51-M-003',
    #         # 'HUS2302-20202-51-M-001',
    #         # 'AMH2020-20202-31-M-001',
    #         # 'HUS2301-20202-31-M-001',
    #         # 'LIT2330-20202-1V-M-001',
    #         # 'PLA2223-20202-11-M-001',
    #         # 'NUR1021C-20202-51-M-001',
    #         # 'NUR2035C-20202-11-M-002',
    #         # 'NUR3145-20202-1V-M-001'
    #     ], courses))
    # print('=' * 80)
    # print(courses)
    # print('=' * 80)
    # TEST
    main()
