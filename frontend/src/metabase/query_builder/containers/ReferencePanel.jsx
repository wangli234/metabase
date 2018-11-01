import React from "react";
import { Box, Flex } from "grid-styled";
import { Motion, spring } from "react-motion";

import colors from "metabase/lib/colors";

import Button from "metabase/components/Button";
import Card from "metabase/components/Card";
import Icon from "metabase/components/Icon";

import { entityListLoader } from "metabase/entities/containers/EntityListLoader";

@entityListLoader({
  entityType: "metrics",
})
class ReferenceComparisons extends React.Component {
  static headerTitle = "Compare";
  render() {
    const { metrics } = this.props;
    return (
      <Box px={3} py={2}>
        {metrics.map(m => (
          <Box key={m.id} my={1} className="text-brand-hover cursor-pointer">
            <Flex align="center">
              <Icon name="insight" color={colors["accent7"]} mr={1} />
              <h3>{m.name}</h3>
            </Flex>
            <p className="m0">{m.description}</p>
          </Box>
        ))}
      </Box>
    );
  }
}

class ReferenceInfo extends React.Component {
  static headerTitle = "Source data info";
  render() {
    const { question } = this.props;
    return (
      <Box px={3}>
        <Box my={2}>
          <h4>{question.query().database().name}</h4>
        </Box>
        <Box my={2}>
          <h4>{question.query().table().display_name}</h4>
          <p>{question.query().table().description}</p>
        </Box>
        <Box my={2}>
          <h4>{question.query().aggregationName()}</h4>
        </Box>
      </Box>
    );
  }
}

class ReferenceTableOfContents extends React.Component {
  static headerTitle = "Guide";
  render() {
    const { question, setView } = this.props;
    const ICON_SIZE = 22;
    return (
      <Box
        className="scroll-y overflow-hidden absolute top bottom left right"
        style={{ paddingTop: 100 }}
      >
        <Card
          p={3}
          mb={2}
          mx={2}
          hoverable
          className="cursor-pointer"
          onClick={() => setView(ReferenceInfo)}
        >
          <Icon name="database" size={ICON_SIZE} />
          <h4>Learn about the source data</h4>
          <p>
            This question is based off of the
            {question.query().database().name} database and the
            {question.query().table().display_name} table
          </p>
        </Card>
        <Card
          p={3}
          mb={2}
          mx={2}
          hoverable
          onClick={() => setView(ReferenceComparisons)}
          className="cursor-pointer"
        >
          <Icon name="insight" size={ICON_SIZE} />
          <h4>Compare</h4>
          <p>Compare this against other metrics or aggregations</p>
        </Card>
        <Card
          p={3}
          mb={2}
          mx={2}
          hoverable
          className="cursor-pointer"
          onClick={() => setView(ReferenceHelp)}
        >
          <Icon name="person" size={ICON_SIZE} />
          <h4>Get help with this</h4>
          <p>
            Talk to a team member. Analysts usually respond within 10 minutes.
          </p>
        </Card>
      </Box>
    );
  }
}

class ReferenceHelp extends React.Component {
  static headerTitle = "Help";
  render() {
    return (
      <Flex
        flex={1}
        flexDirection="column"
        align="center"
        justify="center"
        px={3}
        className="full-height"
      >
        <h3>How would you describe your problem?</h3>
        <Button>I'm confused by this</Button>
        <Button>I need more information on this</Button>
        <Button>Data looks wrong or is missing</Button>
      </Flex>
    );
  }
}

class ReferencePanel extends React.Component {
  state = {
    view: ReferenceTableOfContents,
  };
  render() {
    const { question } = this.props;
    const CurrentView = this.state.view;
    const isTableOfContents = this.state.view === ReferenceTableOfContents;
    return (
      <Card
        style={{ width: 320, minHeight: 600, maxHeight: 800 }}
        className="relative overflow-hidden"
      >
        <Motion
          defaultStyle={{ height: 40 }}
          style={{
            height: isTableOfContents ? spring(140) : spring(40),
          }}
        >
          {({ height }) => (
            <Box p={3} bg={colors["brand"]} color="white" style={{ height }}>
              <Motion
                defaultStyle={{ opacity: 0, x: -5 }}
                style={{
                  opacity: isTableOfContents ? spring(0) : spring(1),
                  x: isTableOfContents ? spring(-5) : spring(0),
                }}
              >
                {({ opacity, x }) => (
                  <Flex align="center">
                    <Box
                      style={{
                        opacity,
                        transform: `translate3d(${x}px, 0px, 0px)`,
                        lineHeight: 1,
                      }}
                      p={1}
                      className="cursor-pointer"
                      onClick={() =>
                        this.setState({ view: ReferenceTableOfContents })
                      }
                    >
                      <Icon name="chevronleft" />
                    </Box>
                    <h2
                      style={{
                        transform: `translate3d(${x}px, 0px, 0px)`,
                      }}
                    >
                      {CurrentView.headerTitle}
                    </h2>
                  </Flex>
                )}
              </Motion>
            </Box>
          )}
        </Motion>
        <CurrentView
          question={question}
          setView={view => this.setState({ view })}
        />
      </Card>
    );
  }
}

export default ReferencePanel;
