import { _job_popen } from "./job";
import * as fs from 'fs'

test('adds 1 + 2 to equal 3', async () => {
    const rcode = await _job_popen(["echo","hellow world"],undefined,"stdout.txt","stderr.txt",{},".",()=>"test")
    const content = fs.readFileSync("stdout.txt").toString()
    expect(rcode).toBe(0)
    console.log(content)
    console.log(typeof content)
    expect(content).toEqual("hellow world\n")
});