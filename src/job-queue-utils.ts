import {HyperflowId} from "./entities";
import * as O from "fp-ts/lib/Option";
import {pipe} from "fp-ts/lib/pipeable";


function createQueueKey(hyperflowId: HyperflowId, suffix: string): string {
    return hyperflowId.hfId.concat('-' + suffix);
}

export const createJobQueueName = (hfId: HyperflowId) => createQueueKey(hfId, 'jq');
export const createProcessingQueueName = (hfId: HyperflowId) => createQueueKey(hfId, 'pq');
export const createCompletedQueueName = (hfId: HyperflowId) => createQueueKey(hfId, 'cq');

export function createJobDescriptionKey(taskId: string): string {
    return taskId.concat('-jd');
}

export function createJobMessageKey(taskId: string): string {
    return taskId.concat('_msg');
}

export const parseEnvVariableOpt = <T>(envVar?: string | undefined) =>
    (parse: (n: string) => T): O.Option<T> => {
        return pipe(
            O.fromNullable(envVar),
            O.map(parse),
        );
    }

export const parseEnvVariable = <T>(envVar: string | undefined) =>
    (parse: (n: string) => T): T => {
        return pipe(
            O.fromNullable(envVar),
            O.fold(
                () => {
                    console.error(`Wrong env value: ${envVar}.`);
                    return process.exit(1)
                },
                parse,
            )
        )
    }
