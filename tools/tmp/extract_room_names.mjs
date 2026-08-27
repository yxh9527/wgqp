import fs from "fs";
import path from "path";

const root = "D:\\project\\wg\\wgServer\\src\\qp\\games";
const result = {};

for (const dir of fs.readdirSync(root, { withFileTypes: true })) {
  if (!dir.isDirectory()) continue;
  const file = path.join(root, dir.name, "config.ts");
  if (!fs.existsSync(file)) continue;
  const text = fs.readFileSync(file, "utf8");
  const items = [];
  const re = /level:\s*(\d+)[\s\S]*?roomName:\s*"([^"]*)"/g;
  let m;
  while ((m = re.exec(text))) {
    items.push({ level: Number(m[1]), roomName: m[2] });
  }
  if (items.length) {
    result[dir.name] = items.sort((a, b) => a.level - b.level);
  }
}

console.log(JSON.stringify(result, null, 2));
