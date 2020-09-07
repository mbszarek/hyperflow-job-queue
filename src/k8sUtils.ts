import * as k8s from "@kubernetes/client-node";
import {V1Job} from "@kubernetes/client-node";
import {
    HyperflowId,
    K8sJobDescription,
    K8sJobMessage,
    K8sJobYaml,
    K8sMultiJobYamlSpec,
    K8sSingleJobYamlSpec
} from "./entities";
import * as O from "fp-ts/lib/Option";
import * as TE from "fp-ts/lib/TaskEither";
import * as E from "fp-ts/lib/Either";
import {Either} from "fp-ts/lib/Either";
import {aglomeratedTasks, k8sJobTemplatePath, k8sNamespace, k8sVolumePath} from "./constants";
import {pipe} from "fp-ts/lib/pipeable";
import {getJobResult} from "./utils";
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import {createJobDescriptionKey, createJobMessageKey} from "./job-queue-utils";
import * as http from "http";
import {IncomingMessage} from "http";
import * as redis from "redis";
import {concat, EMPTY, from, of, partition} from "rxjs";
import {groupBy, toArray} from "rxjs/operators";
import {flatMap} from "rxjs/internal/operators";


type DockerImage = string;
type RedisUrl = string;
type TaskName = string;

const createJobName = (jobMessage: K8sJobDescription): string => {
    const randomPath = Math.random().toString(36).substring(7);
    const jobNameExtracted = jobMessage.name.replace(/_/g, '-');
    const jobName = `${randomPath}-${jobNameExtracted}-${jobMessage.procId}-${jobMessage.firingId}`
    return jobName.replace(/[^0-9a-z-]/gi, '').toLowerCase();
}

const extractCpuRequest = (cpuRequest?: string): string => {
    return "0.5";
}

const extractMemRequest = (memRequest?: string): string => {
    return "50Mi";
}

const interpolate = (yamlTemplate: string, params: Object): string => {
    return yamlTemplate.replace(/\${(\w+)}/g, (_, v) => params[v]);
}

const sendMsgToJob = async (message: K8sJobMessage, redisClient: redis.RedisClient): Promise<number> => {
    return new Promise<number>((resolve, reject) => {
        redisClient.lpush(createJobMessageKey(message.taskId), JSON.stringify(message), (err, reply) => {
            err ? reject(err) : resolve(reply)
        })
    })
}

const getJobDescription = async (taskId: string, redisClient: redis.RedisClient): Promise<K8sJobDescription> => {
    const jobDescriptionJson = await new Promise<string>((resolve, reject) => {
        redisClient.lrange(createJobDescriptionKey(taskId), 0, -1, (err, reply) => {
            err ? reject(err) : resolve(reply[0]);
        })
    })
    return JSON.parse(jobDescriptionJson) as K8sJobDescription;
}

const createK8sJobMessage = (jobDescription: K8sJobDescription): K8sJobMessage => {
    const {
        executorConfig,
        inputs,
        outputs,
        redis_url,
        taskId,
        name,
    } = jobDescription;

    const {
        executable,
        args,
        stdout,
        stderr,
        stdoutAppend,
        stderrAppend,
    } = executorConfig;

    return {
        "executable": executable,
        "args": [].concat(args),
        "inputs": inputs.map(value => JSON.stringify(value)),
        "outputs": outputs.map(value => JSON.stringify(value)),
        "stdout": stdout,
        "stderr": stderr,
        "stdoutAppend": stdoutAppend,
        "stderrAppend": stderrAppend,
        "redis_url": redis_url,
        "taskId": taskId,
        "name": name,
    }
}

const createK8sSingleJobSpec = async (
    hfId: HyperflowId,
    redisUrl: string,
    image: DockerImage,
    jobDescription: K8sJobDescription,
    cpuRequest?: string,
    memRequest?: string
): Promise<K8sSingleJobYamlSpec> => {

    const {
        name,
        wfname,
    } = jobDescription;

    const {taskId} = jobDescription;

    const command = `hflow-job-execute ${taskId} ${redisUrl}`;
    const jobName = createJobName(jobDescription);

    const extractedCpuRequest = extractCpuRequest(cpuRequest);
    const extractedMemRequest = extractMemRequest(memRequest);

    const jobYamlTemplate = fs.readFileSync(k8sJobTemplatePath, 'utf8');

    const params: Object = {
        command: command,
        containerName: image,
        jobName: jobName,
        volumePath: k8sVolumePath,
        cpuRequest: extractedCpuRequest,
        memRequest: extractedMemRequest,
        experimentId: hfId.hfId,
        workflowName: wfname,
        taskName: name,
    };


    const jobYaml = yaml.safeLoad(interpolate(jobYamlTemplate, params));

    const jobMessage = createK8sJobMessage(jobDescription);

    return {
        jobYaml: jobYaml,
        jobMessage: jobMessage,
    }
}


const createK8sMultiJobSpec = async (
    hfId: HyperflowId,
    redisUrl: string,
    image: DockerImage,
    jobDescriptions: Array<K8sJobDescription>,
    name: string,
    cpuRequest?: string,
    memRequest?: string
): Promise<K8sMultiJobYamlSpec> => {

    const {wfname} = jobDescriptions[0];

    const taskIds = jobDescriptions.map(value => value.taskId)

    const command = `hflow-job-execute ${redisUrl} -a ${taskIds.join(' ')}`;
    const jobName = createJobName(jobDescriptions[0]);

    const extractedCpuRequest = extractCpuRequest(cpuRequest);
    const extractedMemRequest = extractMemRequest(memRequest);

    const jobYamlTemplate = fs.readFileSync(k8sJobTemplatePath, 'utf8');

    const params: Object = {
        command: command,
        containerName: image,
        jobName: jobName,
        volumePath: k8sVolumePath,
        cpuRequest: extractedCpuRequest,
        memRequest: extractedMemRequest,
        experimentId: hfId.hfId,
        workflowName: wfname,
        taskName: name,
    };


    const jobYaml = yaml.safeLoad(interpolate(jobYamlTemplate, params));

    const jobMessages = jobDescriptions.map(jobDescription => createK8sJobMessage(jobDescription))

    return {
        jobYaml: jobYaml,
        jobMessages: jobMessages,
    }
}

const segregateJobs = async (
    taskIds: string[],
    redisClient: redis.RedisClient,
): Promise<Array<[[DockerImage, RedisUrl, TaskName], Array<K8sJobDescription>]>> => {
    return from(taskIds).pipe(
        flatMap(taskId => from(getJobDescription(taskId, redisClient))),
        groupBy<K8sJobDescription, [DockerImage, RedisUrl, TaskName]>(jobDescription => {
            return [jobDescription.executorConfig.image, jobDescription.redis_url, jobDescription.name];
        }),
        flatMap(group => {
            return group.pipe(
                toArray(),
                flatMap(value => {
                    return of<[[DockerImage, RedisUrl, TaskName], K8sJobDescription[]]>([group.key, value])
                })
            );
        }),
        toArray()
    ).toPromise();
}

const createK8sSingleJob = async (
    k8sApi: k8s.BatchV1Api,
    jobYaml: K8sJobYaml,
    taskId: string,
    attempt: number,
): Promise<Either<void, { response: http.IncomingMessage; body: V1Job }>> => {
    return pipe(
        TE.tryCatch(
            () => {
                // @ts-ignore
                return k8sApi.createNamespacedJob(k8sNamespace, jobYaml)
            },
            err => {
                E.tryCatch<number, any>(
                    () => {
                        // @ts-ignore
                        const response: IncomingMessage = err.response;
                        const statusCode: number = response.statusCode
                        switch (statusCode) {
                            // if we get 409 or 429 ==> wait and retry
                            case 409: // 'Conflict' -- "Operation cannot be fulfilled on reourcequotas"; bug in k8s?
                            case 429: // 'Too many requests' -- API overloaded
                                // Calculate delay: default 1s, for '429' we should get it in the 'retry-after' header
                                const delay = pipe(
                                    O.fromNullable(response.headers['retry-after']),
                                    O.chain(value => O.tryCatch(() => Number(value))),
                                    O.getOrElse(() => 1),
                                    value => value * 1000,
                                )
                                console.log("Create k8s job", taskId, "HTTP error " + statusCode + " (attempt " + attempt +
                                    "), retrying after " + delay + "ms.");

                                setTimeout(() => createK8sSingleJob(k8sApi, jobYaml, taskId, attempt + 1), delay);
                                break;
                            default:
                                console.error("Err");
                                console.error(err);
                                // console.error(job);
                                let taskEnd = new Date().toISOString();
                                console.log("Task ended with error, time=", taskEnd);
                        }
                    },
                    inErr => {
                        console.error(inErr);
                        throw(inErr);
                    }
                )
            }
        ),
    )();
}

const createK8sMultiJob = async (
    k8sApi: k8s.BatchV1Api,
    jobYaml: K8sJobYaml,
    taskIds: string[],
    attempt: number,
): Promise<Either<void, { response: http.IncomingMessage; body: V1Job }>> => {
    return pipe(
        TE.tryCatch(
            () => {
                // @ts-ignore
                return k8sApi.createNamespacedJob(k8sNamespace, jobYaml);
            },
            err => {
                E.tryCatch<number, any>(
                    () => {
                        // @ts-ignore
                        const response: IncomingMessage = err.response;
                        const statusCode: number = response.statusCode
                        switch (statusCode) {
                            // if we get 409 or 429 ==> wait and retry
                            case 409: // 'Conflict' -- "Operation cannot be fulfilled on reourcequotas"; bug in k8s?
                            case 429: // 'Too many requests' -- API overloaded
                                // Calculate delay: default 1s, for '429' we should get it in the 'retry-after' header
                                const delay = pipe(
                                    O.fromNullable(response.headers['retry-after']),
                                    O.chain(value => O.tryCatch(() => Number(value))),
                                    O.getOrElse(() => 1),
                                    value => value * 1000,
                                )
                                console.log("Create k8s job", taskIds, "HTTP error " + statusCode + " (attempt " + attempt +
                                    "), retrying after " + delay + "ms.");

                                setTimeout(() => createK8sMultiJob(k8sApi, jobYaml, taskIds, attempt + 1), delay);
                                break;
                            default:
                                console.error("Err");
                                console.error(err);
                                // console.error(job);
                                let taskEnd = new Date().toISOString();
                                console.log("Task ended with error, time=", taskEnd);
                        }
                    },
                    inErr => {
                        console.error(inErr);
                        throw(inErr);
                    }
                )
            }
        ),
    )();
};

const awaitK8sJob = async (taskIds: string[], redisClient: redis.RedisClient): Promise<number> => {
    const jobResult = await getJobResult(redisClient, taskIds[0], 0);
    return 2;
};

export const submitK8sJobs = async (
    hfId: HyperflowId,
    kubeConfig: k8s.KubeConfig,
    redisClient: redis.RedisClient,
    taskIds: string[],
): Promise<number> => {
    const segregatedJobs = from(segregateJobs(taskIds, redisClient)).pipe(
        flatMap(value => value),
    )
    const [tasksToAglomerate, singularTasks] = partition(segregatedJobs, value => {
        return aglomeratedTasks.has(value[0][2]); // value[0][2] - taskName
    })

    const k8sApi = kubeConfig.makeApiClient(k8s.BatchV1Api);

    const firstTask = tasksToAglomerate.pipe(
        flatMap(value => {
            const [[image, redisUrl, taskName], jobDescription] = value;
            return from(createK8sMultiJobSpec(hfId, redisUrl, image, jobDescription, taskName));
        }),
        flatMap(value => {
            const {jobYaml, jobMessages} = value;
            if (process.env.HF_VAR_K8S_TEST) {
                console.log(JSON.stringify(jobYaml, null, 4));
                const stringifiedMessages = jobMessages.map(value => JSON.stringify(value, null, 2));
                stringifiedMessages.forEach(value => console.log(value));
                return EMPTY;
            }
            const taskStartDate = new Date().toISOString();
            console.log('Starting tasks', taskIds, 'time=' + taskStartDate);
            return from(createK8sMultiJob(k8sApi, jobYaml, taskIds, 0)).pipe(
                flatMap(_ => {
                    return from(jobMessages).pipe(
                        flatMap(jobMessage => sendMsgToJob(jobMessage, redisClient))
                    );
                })
            );
        })
    );

    const secondTask = singularTasks.pipe(
        flatMap(value => {
            const [[image, redisUrl, _], jobDescriptions] = value;
            return from(jobDescriptions).pipe(
                flatMap(jobDescription => from(createK8sSingleJobSpec(hfId, redisUrl, image, jobDescription)))
            )
        }),
        flatMap(value => {
            const {jobYaml, jobMessage} = value;
            if (process.env.HF_VAR_K8S_TEST) {
                console.log(JSON.stringify(jobYaml, null, 4));
                console.log(JSON.stringify(jobMessage, null, 2));
                return EMPTY;
            }
            const taskStartDate = new Date().toISOString();
            console.log('Starting task', jobMessage.taskId, 'time=' + taskStartDate);
            return from(createK8sSingleJob(k8sApi, jobYaml, jobMessage.taskId, 0)).pipe(
                flatMap(_ => sendMsgToJob(jobMessage, redisClient)),
            );
        })
    );

    return concat(firstTask, secondTask).toPromise();

}