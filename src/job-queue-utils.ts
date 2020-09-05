import {HyperflowId} from "./entities";
import * as O from "fp-ts/lib/Option";
import {pipe} from "fp-ts/lib/pipeable";


export function createQueueKey(hyperflowId: HyperflowId, suffix: string): string {
    return hyperflowId.hfId.concat('_' + suffix);
}

export function createJobDescriptionKey(taskId: string): string {
    return taskId.concat('-jd');
}

export function createJobMessageKey(taskId: string): string {
    return taskId.concat('_msg');
}

export const parseEnvVariableOpt = <T>(envVar: string | undefined) =>
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
