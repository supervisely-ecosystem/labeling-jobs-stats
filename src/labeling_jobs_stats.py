import os
from dateutil import parser
import supervisely_lib as sly
from supervisely_lib.labeling_jobs.utils import is_completed, is_stopped, is_not_started, is_on_labeling, is_on_review, \
    is_review_started, is_labeling_started, total_items_count, labeled_items_count, reviewed_items_count, \
    accepted_items_count, rejected_items_count, get_job_url



my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])

def count_jobs_statuses(jobs):
    columns = ["#", "job status", "quantity", "% of total", "description"]

    count_total = len(jobs)
    count_total_name = "TOTAL"
    count_total_desc = "the total number of jobs in current team"

    count_stopped = 0
    count_stopped_name = "STOPPED"
    count_stopped_desc = "stopped jobs"

    count_completed = 0
    count_completed_name = "COMPLETED"
    count_completed_desc = "completed jobs"

    count_pending = 0
    count_pending_name = "PENDING"
    count_pending_desc = "job is created but not started"

    count_labeling_zero = 0
    count_labeling_zero_name = "IN PROGRESS BUT NOTHING IS LABELED"
    count_labeling_zero_desc = "job is started but no activity yet"

    count_labeling_started = 0
    count_labeling_started_name = "IN PROGRESS WITH LABELS"
    count_labeling_started_desc = "at least one image is marked as finished"

    count_labeling_ip = 0
    count_labeling_ip_name = "IN PROGRESS"
    count_labeling_ip_desc = "IN PROGRESS BUT NOTHING IS LABELED + IN PROGRESS WITH LABELS"

    count_review_zero = 0
    count_review_zero_name = "ON REVIEW BUT NOTHING IS REVIEWED"
    count_review_zero_desc = "no review activity yet"

    count_review_ip = 0
    count_review_ip_name = "ON REVIEW WITH SOME FINISHED ITEMS"
    count_review_ip_desc = "some images are reviewed"

    count_review = 0
    count_review_name = "ON REVIEW"
    count_review_desc = "ON REVIEW BUT NOTHING IS REVIEWED + ON REVIEW WITH SOME FINISHED ITEMS"

    for job in jobs:
        if is_completed(job):
            count_completed += 1
        elif is_stopped(job):
            count_stopped += 1
        elif is_not_started(job):
            count_pending += 1
        elif is_on_labeling(job):
            count_labeling_ip += 1
            if is_labeling_started(job):
                count_labeling_started += 1
            else:
                count_labeling_zero += 1
        elif is_on_review(job):
            count_review += 1
            if is_review_started(job):
                count_review_ip += 1
            else:
                count_review_zero += 1
        else:
            raise RuntimeError("Unhandled job status: {}".format(str(job)))

    def _percent(val):
        return round(val * 100 / count_total, 1)

    statuses_table = {
        "columns": columns,
        "data": [
            [1, count_total_name, count_total, _percent(count_total), count_total_desc],
            [2, count_stopped_name, count_stopped, _percent(count_stopped), count_stopped_desc],
            [3, count_completed_name, count_completed, _percent(count_completed), count_completed_desc],
            [4, count_pending_name, count_pending, _percent(count_pending), count_pending_desc],
            [5, count_labeling_zero_name, count_labeling_zero, _percent(count_labeling_zero), count_labeling_zero_desc],
            [6, count_labeling_started_name, count_labeling_started, _percent(count_labeling_started), count_labeling_started_desc],
            [7, count_labeling_ip_name, count_labeling_ip, _percent(count_labeling_ip), count_labeling_ip_desc],
            [8, count_review_zero_name, count_review_zero, _percent(count_review_zero), count_review_zero_desc],
            [9, count_review_ip_name, count_review_ip, _percent(count_review_ip), count_review_ip_desc],
            [10, count_review_name, count_review, _percent(count_review), count_review_desc],
        ]
    }
    return statuses_table

def count_images_statuses(jobs):
    columns = ["#", "image status", "quantity", "% of total", "description"]

    total = 0
    total_name = "TOTAL"
    total_desc = "total images count in all labeling jobs"

    labeled = 0
    labeled_name = "LABELED"
    labeled_desc = "finished by labelers"

    accepted = 0
    accepted_name = "ACCEPTED"
    accepted_desc = "accepted by reviewer"

    rejected = 0
    rejected_name = "REJECTED"
    rejected_desc = "rejected by reviewer"

    reviewed = 0
    reviewed_name = "REVIEWED"
    reviewed_desc = "accepted images + rejected images"

    for job in jobs:
        total += total_items_count(job)
        labeled += labeled_items_count(job)
        reviewed += reviewed_items_count(job)
        accepted += accepted_items_count(job)
        rejected += rejected_items_count(job)
        if reviewed != accepted + rejected:
            raise RuntimeError("Wrong image statuses calculation")

    def _percent(val):
        return round(val * 100 / total, 1)

    image_statuses_table = {
        "columns": columns,
        "data": [
            [1, total_name, total, _percent(total), total_desc],
            [2, labeled_name, labeled, _percent(labeled), labeled_desc],
            [3, reviewed_name, reviewed, _percent(reviewed), reviewed_desc],
            [4, accepted_name, accepted, _percent(accepted), accepted_desc],
            [5, rejected_name, rejected, _percent(rejected), rejected_desc]
        ]
    }
    return image_statuses_table

def count_jobs_table(server_address, jobs, stats):
    columns = ['ID', 'NAME', 'STATUS', 'CREATED_AT', 'TOTAL', 'LABELED', 'REVIEWED', 'ACCEPTED', 'REJECTED']
    data = []
    for job, stat in zip(jobs, stats):
        data_row = []
        data_row.append(job.id)
        data_row.append('<a href="{0}" rel="noopener noreferrer" target="_blank">{1}</a>'
                            .format(get_job_url(server_address, job), job.name))
        data_row.append(job.status)
        data_row.append(parser.parse(job.created_at).strftime('%Y/%m/%d/ %H:%M'))
        data_row.append(total_items_count(job))
        data_row.append(labeled_items_count(job))
        data_row.append(reviewed_items_count(job))
        data_row.append(accepted_items_count(job))
        data_row.append(rejected_items_count(job))
        data.append(data_row)

    jobs_table = {
        "columns": columns,
        "data": data
    }
    return jobs_table

@my_app.callback("preprocessing")
@sly.timeit
def preprocessing(api: sly.Api, task_id, context, state, app_logger):
    team = api.team.get_info_by_id(TEAM_ID)
    jobs = api.labeling_job.get_list(team.id)
    if len(jobs) == 0:
        raise RuntimeError("There are no labeling jobs in current team {!r}".format(team.name))
    stats = [api.labeling_job.get_stats(job.id) for job in jobs]

    job_statuses_table = count_jobs_statuses(jobs)
    image_statuses_table = count_images_statuses(jobs)
    jobs_table = count_jobs_table(api.server_address, jobs, stats)

    fields = [
        {"field": "data.jobStatusesTable", "payload": job_statuses_table},
        {"field": "data.imageStatusesTable", "payload": image_statuses_table},
        {"field": "data.jobsTable", "payload": jobs_table},
    ]
    api.task.set_fields(task_id, fields)

    my_app.stop()

def main():
    initial_events = [{"state": None, "context": None, "command": "preprocessing"}]

    # Run application service
    my_app.run(initial_events=initial_events)


if __name__ == "__main__":
    sly.main_wrapper("main", main)