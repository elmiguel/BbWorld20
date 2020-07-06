select
    *
from cdm_lms.person p
where p.stage:user_id::string = {user_id}