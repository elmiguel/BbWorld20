select * from cdm_lms.activity a
left join cdm_lms.person p
    on p.global_person_id = a.person_id
where p.stage:user_id::string = {user_id};