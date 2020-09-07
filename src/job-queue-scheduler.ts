import * as O from "fp-ts/lib/Option";
import {HyperflowId} from "./entities";
import * as redis from 'redis';
import {pipe} from "fp-ts/lib/pipeable";
import {EMPTY, Observable, Observer, of} from "rxjs";
import {bufferTime} from "rxjs/operators";
import * as k8s from '@kubernetes/client-node';
import {maxBatchJobs, pollingInterval, redisURL} from "./constants";
import {flatMap} from "rxjs/internal/operators";
import {submitK8sJobs} from "./k8sUtils";
import {createJobQueueName, createProcessingQueueName} from "./job-queue-utils";

const rcl = pipe(
    redisURL,
    O.fold(
        () => redis.createClient(),
        str => redis.createClient(str)
    )
)

const pollQueue = (jobQueue: string, processingQueue: string): Observable<string> => {
    return new Observable<string>(subscriber => {
        const queryRedis = () => {
            rcl.brpoplpush(jobQueue, processingQueue, pollingInterval, (err, taskId) => {
                if (err) {
                    subscriber.error(err);
                } else {
                    if (taskId) {
                        subscriber.next(taskId);
                        queryRedis();
                    } else {
                        subscriber.complete();
                    }
                }
            });
        }
        queryRedis();
    })
};


const subscribeToHflowId = (): void => {
    console.log("Subscribing to hflow");
    const redisHflowId = rcl.duplicate();

    redisHflowId.config('get', 'notify-keyspace-events', (_, conf) => {
        const subscribe = () => {
            redisHflowId.psubscribe('__keyspace@0__:hflow:*');

            redisHflowId.on('pmessage', (channel, message) => {
                const regex = /^.*hflow:(.+)$/gm;
                const match = regex.exec(message);
                console.log(`New HFID: ${match[1]}`)
                const hfId: HyperflowId = {
                    hfId: match[1],
                };
                pollingFunction(hfId);
            });
        }

        if (conf[0].indexOf('notify-keyspace-events') !== -1) {
            subscribe();
        } else {
            console.log("notify-keyspace-events not set, configuring...");
            redisHflowId.config('set', 'notify-keyspace-events', 'Kx', _ => {
                subscribe();
            })
        }
    });
}

subscribeToHflowId();

const kubeConfig = new k8s.KubeConfig();
kubeConfig.loadFromDefault();

const observer = (hfId: HyperflowId): Observer<string[]> => {
    return {
        complete(): void {
            console.log("Completed");
        },
        error(err: any): void {
            console.error(err);
        },
        next(taskIds: string[]): void {
            submitK8sJobs(hfId, kubeConfig, rcl.duplicate(), taskIds);
        }
    }
}

const pollingFunction = (hfId: HyperflowId) => {
    pollQueue(createJobQueueName(hfId), createProcessingQueueName(hfId)).pipe(
        bufferTime(pollingInterval * 1000, undefined, maxBatchJobs),
        flatMap(value => {
            if (value.length === 0) {
                return EMPTY;
            } else {
                return of(value);
            }
        })
    ).subscribe(observer(hfId));
}

