select 
    c.name,
    ca.first_accessed_time,
    ca.last_accessed_time,
    sum(ca.duration_sum) / (60 * 60) as duration_minutes
from cdm_lms.course_activity ca
    left join cdm_lms.person p
        on p.global_person_id = ca.person_id
      left join cdm_lms.course c
        on c.id = ca.course_id
where p.stage:user_id::string = 'mbechtel'
group by c.name, ca.first_accessed_time, ca.last_accessed_time;