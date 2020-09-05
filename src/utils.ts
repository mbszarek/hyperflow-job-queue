import * as O from 'fp-ts/lib/Option';
import {RedisClient} from "redis";
import {JobResult} from "./entities";

export const identity = <T>(arg: T): T => arg;

export const orElse = <T>(ifEmpty: () => O.Option<T>) =>
    (arg: O.Option<T>): O.Option<T> => {
        if (O.isSome(arg)) {
            return arg;
        } else {
            ifEmpty();
        }
    }

export const getJobResult = async (redisClient: RedisClient, taskId: string, timeout: number): Promise<JobResult> => {
    return new Promise(function (resolve, reject) {
        redisClient.blpop(taskId, timeout, function (err, reply) {
            err ? reject(err) : resolve({
                message: reply[0],
                code: parseInt(reply[1]),
            });
        });
    });
}