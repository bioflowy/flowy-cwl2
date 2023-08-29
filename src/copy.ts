import cloneDeep from 'lodash/cloneDeep';

export function deepcopy<T>(value:T):T {
    return cloneDeep(value)
}