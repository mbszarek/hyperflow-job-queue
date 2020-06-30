import * as O from 'fp-ts/lib/Option';

export const identity = <T>(arg: T): T => arg;

export const orElse = <T>(ifEmpty: () => O.Option<T>) =>
    (arg: O.Option<T>): O.Option<T> => {
        if (O.isSome(arg)) {
            return arg;
        } else {
            ifEmpty();
        }
    }