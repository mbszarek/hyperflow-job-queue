import * as O from "fp-ts/lib/Option";
import {parseEnvVariable, parseEnvVariableOpt} from "./job-queue-utils";
import {HyperflowId, K8sExecutorConfig, RedisNotifyKeyspaceEvents} from "./entities";
import * as redis from 'redis';
import {pipe} from "fp-ts/lib/pipeable";
import {createQueueKey} from './job-queue-utils';
import {Observable, Observer} from "rxjs";
import {groupBy, mergeMap, toArray} from "rxjs/operators";
import * as k8s from '@kubernetes/client-node';
import * as fs from 'fs';
import {identity} from "./utils";
import {defaultContainerName, jobQueueName, pollingInterval, redisURL} from "./contants";

const rcl = pipe(
    redisURL,
    O.fold(
        () => redis.createClient(),
        str => redis.createClient(str)
    )
)

const redisSub = rcl.duplicate();

const pollQueue = (queueName: string): Observable<string> => {
    return new Observable<string>(subscriber => {
        rcl.lrange(queueName, 0, -1, (err, tasks) => {
            Promise
                .all(tasks.map(elem => {
                    return new Promise((resolve, _) => {
                        rcl.get(elem, (err, res) => {
                            if (err) {
                                subscriber.error(err);
                            } else {
                                subscriber.next(res);
                            }
                            resolve();
                        })
                    })
                }))
                .then(_ => subscriber.complete());
        })
    })
};


// Redis subscription
redisSub.config('get', 'notify-keyspace-events', (_, conf) => {
    const subscribe = () => {
        redisSub.subscribe(`__keyspace@0__:${jobQueueName}`);

        redisSub.on('message', _ => {
            pollingFunction();
        })
    }

    if (conf[0].indexOf('notify-keyspace-events') !== -1) {
        subscribe()
    } else {
        console.log("notify-keyspace-events not set, configuring...")
        redisSub.config('set', 'notify-keyspace-events', 'Kx', _ => {
            subscribe()
        })
    }
});

let pollingExecuting = false;

const observer: Observer<string[]> = {
    complete(): void {
        console.log("SDFFDS");
        pollingExecuting = false;
    },
    error(err: any): void {
        console.error(err);
    },
    next(value: string[]): void {
        console.log(value);
    }


}

const kubeConfig = new k8s.KubeConfig();
kubeConfig.loadFromDefault();

const pollingFunction = () => {
    if (!pollingExecuting) {
        pollingExecuting = true;
        setTimeout(
            () => {
                pollQueue(jobQueueName)
                    .pipe(
                        groupBy(value => value),
                        mergeMap(group => group.pipe(toArray())),
                    )
                    .subscribe(observer);
            },
            pollingInterval,
        )
    }
};


